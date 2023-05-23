// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use crate::{
	column::{hash_key, ColId, Column, TablesRef},
	compress::Compress,
	db::{RcValue},
	display::hex,
	error::{Error, Result},
	options::{Metadata, Options, DEFAULT_COMPRESSION_THRESHOLD},
	parking_lot::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard},
	stats::{ColumnStatSummary, ColumnStats},
	log::{Log, LogAction, LogOverlays, LogQuery, LogReader, LogWriter},
	index::{Address, IndexTable, PlanOutcome, TableId as IndexTableId},
	table::{
		key::{TableKey, TableKeyQuery},
		Entry as ValueTableEntry, Value, ValueTable,
	},
	Key,
	Operation,
};
use std::{
	collections::VecDeque,
	path::PathBuf,
	sync::{
		atomic::{AtomicU64, Ordering},
		Arc,
	},
};

const MIN_INDEX_BITS: u8 = 16;

pub type NodeAddress = u64;
pub type Children = Vec<NodeAddress>;

#[derive(Debug, PartialEq, Eq)]
pub enum NodeRef {
	New(NewNode),
	Existing(NodeAddress),
}

#[derive(Debug, PartialEq, Eq)]
pub struct NewNode {
	pub data: Vec<u8>,
	pub children: Vec<NodeRef>,
}

#[derive(Debug)]
struct Tables {
	index: IndexTable,
	value: Vec<ValueTable>,
}

#[derive(Debug)]
struct Reindex {
	queue: VecDeque<IndexTable>,
	progress: AtomicU64,
}

#[derive(Debug)]
pub struct MultiTreeColumn {
	col: ColId,
	//index: RwLock<IndexTable>,
	//values: RwLock<Vec<ValueTable>>,
	tables: RwLock<Tables>,
	reindex: RwLock<Reindex>,
	path: PathBuf,
	compression: Compress,
}

impl MultiTreeColumn {
	pub fn open(
		col: ColId,
		value: Vec<ValueTable>,
		options: &Options,
		metadata: &Metadata,
	) -> Result<Self> {
		let (index, reindexing, stats) = Self::open_index(&options.path, col)?;
		let path = &options.path;
		let col_options = &metadata.columns[col as usize];
		Ok(MultiTreeColumn {
			col,
			//index: RwLock::new(index),
			//values: RwLock::new(value),
			tables: RwLock::new(Tables { index, value }),
			reindex: RwLock::new(Reindex { queue: reindexing, progress: AtomicU64::new(0) }),
			path: path.into(),
			compression: Compress::new(
				col_options.compression,
				options
					.compression_threshold
					.get(&col)
					.copied()
					.unwrap_or(DEFAULT_COMPRESSION_THRESHOLD),
			),
		})
	}

	fn open_index(
		path: &std::path::Path,
		col: ColId,
	) -> Result<(IndexTable, VecDeque<IndexTable>, ColumnStats)> {
		let mut reindexing = VecDeque::new();
		let mut top = None;
		let mut stats = ColumnStats::empty();
		for bits in (MIN_INDEX_BITS..65).rev() {
			let id = IndexTableId::new(col, bits);
			if let Some(table) = IndexTable::open_existing(path, id)? {
				if top.is_none() {
					stats = table.load_stats()?;
					log::trace!(target: "parity-db", "Opened main index {}", table.id);
					top = Some(table);
				} else {
					log::trace!(target: "parity-db", "Opened stale index {}", table.id);
					reindexing.push_front(table);
				}
			}
		}
		let table = match top {
			Some(table) => table,
			None => IndexTable::create_new(path, IndexTableId::new(col, MIN_INDEX_BITS)),
		};
		Ok((table, reindexing, stats))
	}

	pub fn flush(&self) -> Result<()> {
		let tables = self.tables.read();
		tables.index.flush()?;
		for t in tables.value.iter() {
			t.flush()?;
		}
		Ok(())
	}

	fn trigger_reindex<'a, 'b>(
		tables: RwLockUpgradableReadGuard<'a, Tables>,
		reindex: RwLockUpgradableReadGuard<'b, Reindex>,
		path: &std::path::Path,
	) -> (RwLockUpgradableReadGuard<'a, Tables>, RwLockUpgradableReadGuard<'b, Reindex>) {
		let mut tables = RwLockUpgradableReadGuard::upgrade(tables);
		let mut reindex = RwLockUpgradableReadGuard::upgrade(reindex);
		log::info!(
			target: "parity-db",
			"Started reindex for {}",
			tables.index.id,
		);
		// Start reindex
		let new_index_id =
			IndexTableId::new(tables.index.id.col(), tables.index.id.index_bits() + 1);
		let new_table = IndexTable::create_new(path, new_index_id);
		let old_table = std::mem::replace(&mut tables.index, new_table);
		reindex.queue.push_back(old_table);
		(
			RwLockWriteGuard::downgrade_to_upgradable(tables),
			RwLockWriteGuard::downgrade_to_upgradable(reindex),
		)
	}

	pub fn write_plan(
		&self,
		change: &Operation<Key, RcValue>,
		log: &mut LogWriter,
	) -> Result<PlanOutcome> {
		let tables = self.tables.upgradable_read();
		let reindex = self.reindex.upgradable_read();
		/* let existing = Self::search_all_indexes(change.key(), &tables, &reindex, log)?;
		if let Some((table, sub_index, existing_address)) = existing {
			self.write_plan_existing(&tables, change, log, table, sub_index, existing_address)
		} else */ {
			match change {
				Operation::Set(key, value) => {
					let (r, _, _) =
						self.write_plan_new(tables, reindex, key, value.as_ref(), log)?;
					Ok(r)
				},
				_ => {
					Ok(PlanOutcome::Skipped)
				},
			}
		}
	}

	fn write_plan_new<'a, 'b>(
		&self,
		mut tables: RwLockUpgradableReadGuard<'a, Tables>,
		mut reindex: RwLockUpgradableReadGuard<'b, Reindex>,
		key: &Key,
		value: &[u8],
		log: &mut LogWriter,
	) -> Result<(
		PlanOutcome,
		RwLockUpgradableReadGuard<'a, Tables>,
		RwLockUpgradableReadGuard<'b, Reindex>,
	)> {
		let stats = None;//self.collect_stats.then_some(&self.stats);
		let table_key = TableKey::Partial(*key);
		let address = Column::write_new_value_plan(
			&table_key,
			self.as_ref(&tables.value),
			value,
			log,
			stats,
		)?;
		let mut outcome = PlanOutcome::Written;
		while let PlanOutcome::NeedReindex =
			tables.index.write_insert_plan(key, address, None, log)?
		{
			log::debug!(target: "parity-db", "{}: Index chunk full {}", tables.index.id, hex(key));
			(tables, reindex) = Self::trigger_reindex(tables, reindex, self.path.as_path());
			outcome = PlanOutcome::NeedReindex;
		}
		Ok((outcome, tables, reindex))
	}

	pub fn enact_plan(&self, action: LogAction, log: &mut LogReader) -> Result<()> {
		let tables = self.tables.read();
		let reindex = self.reindex.read();
		match action {
			LogAction::InsertIndex(record) => {
				if tables.index.id == record.table {
					tables.index.enact_plan(record.index, log)?;
				} else if let Some(table) = reindex.queue.iter().find(|r| r.id == record.table) {
					table.enact_plan(record.index, log)?;
				} else {
					// This may happen when removal is planed for an old index when reindexing.
					// We can safely skip the removal since the new index does not have the entry
					// anyway and the old index is already dropped.
					log::debug!(
						target: "parity-db",
						"Missing index {}. Skipped",
						record.table,
					);
					IndexTable::skip_plan(log)?;
				}
			},
			LogAction::InsertValue(record) => {
				tables.value[record.table.size_tier() as usize].enact_plan(record.index, log)?;
			},
			// This should never happen, unless something has modified the log file while the
			// database is running. Existing logs should be validated with `validate_plan` on
			// startup.
			_ => return Err(Error::Corruption("Unexpected log action".into())),
		}
		Ok(())
	}

	pub fn validate_plan(&self, action: LogAction, log: &mut LogReader) -> Result<()> {
		let tables = self.tables.upgradable_read();
		let reindex = self.reindex.upgradable_read();
		match action {
			LogAction::InsertIndex(record) => {
				if tables.index.id == record.table {
					tables.index.validate_plan(record.index, log)?;
				} else if let Some(table) = reindex.queue.iter().find(|r| r.id == record.table) {
					table.validate_plan(record.index, log)?;
				} else {
					if record.table.index_bits() < tables.index.id.index_bits() {
						// Insertion into a previously dropped index.
						log::warn!( target: "parity-db", "Index {} is too old. Current is {}", record.table, tables.index.id);
						return Err(Error::Corruption("Unexpected log index id".to_string()))
					}
					// Re-launch previously started reindex
					// TODO: add explicit log records for reindexing events.
					log::warn!(
						target: "parity-db",
						"Missing table {}, starting reindex",
						record.table,
					);
					let lock = Self::trigger_reindex(tables, reindex, self.path.as_path());
					std::mem::drop(lock);
					return self.validate_plan(LogAction::InsertIndex(record), log)
				}
			},
			LogAction::InsertValue(record) => {
				tables.value[record.table.size_tier() as usize].validate_plan(record.index, log)?;
			},
			_ => {
				log::error!(target: "parity-db", "Unexpected log action");
				return Err(Error::Corruption("Unexpected log action".to_string()))
			},
		}
		Ok(())
	}

	pub fn complete_plan(&self, log: &mut LogWriter) -> Result<()> {
		let tables = self.tables.read();
		for t in tables.value.iter() {
			t.complete_plan(log)?;
		}
		Ok(())
	}

	pub fn refresh_metadata(&self) -> Result<()> {
		let tables = self.tables.read();
		for t in tables.value.iter() {
			t.refresh_metadata()?;
		}
		Ok(())
	}

	pub fn drop_index(&self, id: IndexTableId) -> Result<()> {
		log::debug!(target: "parity-db", "Dropping {}", id);
		let mut reindex = self.reindex.write();
		if reindex.queue.front_mut().map_or(false, |index| index.id == id) {
			let table = reindex.queue.pop_front();
			reindex.progress.store(0, Ordering::Relaxed);
			table.unwrap().drop_file()?;
		} else {
			log::warn!(target: "parity-db", "Dropping invalid index {}", id);
			return Ok(())
		}
		log::debug!(target: "parity-db", "Dropped {}", id);
		Ok(())
	}

	pub fn as_ref<'a>(&'a self, tables: &'a [ValueTable]) -> TablesRef<'a> {
		TablesRef {
			tables,
			preimage: false,//self.preimage,
			col: self.col,
			ref_counted: false,//self.ref_counted,
			compression: &self.compression,
		}
	}
}

fn write_children(children: Vec<NodeRef>, tables: TablesRef, writer: &mut LogWriter) -> Result<Vec<u8>> {
	let mut data = Vec::new();
	for child in children {
		let address = match child {
			NodeRef::New(node) => {
				write_node(node, tables, writer)?
			},
			NodeRef::Existing(address) => {
				address
			}
		};
		let mut data_buf = [0u8; 8];
		data_buf.copy_from_slice(&address.to_le_bytes());
		data.append(&mut data_buf.to_vec());
	}
	Ok(data)
}

fn write_node(node: NewNode, tables: TablesRef, writer: &mut LogWriter) -> Result<NodeAddress> {
	let data = [node.data, write_children(node.children, tables, writer)?].concat();

	let table_key = TableKey::NoHash;
	let address = Column::write_new_value_plan(
		&table_key,
		tables,
		data.as_ref(),
		writer,
		None,//stats,
	)?;

	Ok(address.as_u64())
}

pub mod commit_overlay {
	use super::*;
	use crate::{
		column::{ColId, Column},
		db::{MultiTreeCommitOverlay, Operation, RcKey, RcValue},
		error::Result,
	};

	#[derive(Debug)]
	pub struct MultiTreeChangeSet {
		pub col: ColId,
		pub changes: Vec<Operation<Key, RcValue>>,
	}

	impl MultiTreeChangeSet {
		pub fn new(col: ColId) -> Self {
			MultiTreeChangeSet { col, changes: Default::default() }
		}

		pub fn push(
			&mut self, 
			column: &Column, 
			change: Operation<Value, Value>, 
			options: &Options, 
			db_version: u32,
			log: &Log,
			bytes: &mut u64,
		) -> Result<()> {
			match column {
				Column::MultiTree(multitree) => {
					match change {
						Operation::InsertTree(key, node) => {
							// Traverse tree acquiring table addresses for each node. Write LogWriter for this.
							// Then add a single operation to changes which is the Set of the root key to root data (which includes child NodeAddresses etc).
							// Don't need any Operations for child nodes or anything added to the commit overlay because they have already been added to a log record (and hence log overlay).

							let mut writer = log.begin_record();
							let tables = multitree.tables.upgradable_read();

							let data = [node.data, write_children(node.children, multitree.as_ref(&tables.value), &mut writer)?].concat();

							multitree.complete_plan(&mut writer)?;

							//let record_id = writer.record_id();

							let l = writer.drain();

							*bytes += log.end_record(l)?;

							let salt = options.salt.unwrap_or_default();
							let hash_key = |key: &[u8]| -> Key {
								hash_key(key, &salt, options.columns[self.col as usize].uniform, db_version)
							};

							self.changes.push(Operation::Set(hash_key(key.as_ref()), data.into()));
						},
						Operation::RemoveTree(_key) => {
							return Err(Error::InvalidInput(format!("RemoveTree not implemented yet")))
						},
						_ => {
							return Err(Error::InvalidInput(format!("Invalid operation for column {}", self.col)))
						}
					}
				},
				_ => {
					return Err(Error::InvalidInput(format!("Column is not MultiTree")))
				}
			}

			Ok(())
		}

		pub fn copy_to_overlay(
			&self,
			overlay: &mut MultiTreeCommitOverlay,
			record_id: u64,
			bytes: &mut usize,
			options: &Options,
		) -> Result<()> {
			for change in self.changes.iter() {
				match change {
					Operation::Dereference(..) | Operation::Reference(..) => {
						return Err(Error::InvalidInput(format!(
							"Operation not supported for column {}",
							self.col
						)))
					},
					Operation::Set(k, v) => {
						// Get a set operation from each InsertTree. This is for the root node.
						*bytes += k.len();
						*bytes += v.value().len();
						overlay.0.insert(*k, (record_id, Some(v.clone())));
					},
					Operation::InsertTree(key, node) => {
						return Err(Error::InvalidInput(format!("Unexpected operation")))
					},
					Operation::RemoveTree(key) => {
						return Err(Error::InvalidInput(format!("RemoveTree not implemented yet")))
					}
				}
			}
			Ok(())
		}

		pub fn clean_overlay(&mut self, overlay: &mut MultiTreeCommitOverlay, record_id: u64) {
			use std::collections::hash_map::Entry;
			for change in self.changes.iter() {
				match change {
					Operation::Set(k, _) => {
						if let Entry::Occupied(e) = overlay.0.entry(*k) {
							if e.get().0 == record_id {
								e.remove_entry();
							}
						}
					},
					Operation::Reference(..) | Operation::Dereference(..) | Operation::InsertTree(..) | Operation::RemoveTree(..) => (),
				}
			}
		}

		pub fn write_plan(
			&mut self,
			multitree: &MultiTreeColumn,
			writer: &mut LogWriter,
			ops: &mut u64,
			reindex: &mut bool,
		) -> Result<()> {
			for change in self.changes.iter() {
				if let PlanOutcome::NeedReindex = multitree.write_plan(change, writer)? {
					// Reindex has triggered another reindex.
					*reindex = true;
				}
				*ops += 1;
			}
			Ok(())
		}
	}
}