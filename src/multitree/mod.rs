// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use crate::{
	column::{ColId, Column, TablesRef},
	compress::Compress,
	db::{RcValue},
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
		/* let tables = self.tables.upgradable_read();
		let reindex = self.reindex.upgradable_read();
		let existing = Self::search_all_indexes(change.key(), &tables, &reindex, log)?;
		if let Some((table, sub_index, existing_address)) = existing {
			self.write_plan_existing(&tables, change, log, table, sub_index, existing_address)
		} else {
			match change {
				Operation::Set(key, value) => {
					let (r, _, _) =
						self.write_plan_new(tables, reindex, key, value.as_ref(), log)?;
					Ok(r)
				},
				Operation::Dereference(key) => {
					log::trace!(target: "parity-db", "{}: Deleting missing key {}", tables.index.id, hex(key));
					if self.collect_stats {
						self.stats.remove_miss();
					}
					Ok(PlanOutcome::Skipped)
				},
				Operation::Reference(key) => {
					log::trace!(target: "parity-db", "{}: Ignoring increase rc, missing key {}", tables.index.id, hex(key));
					if self.collect_stats {
						self.stats.reference_increase_miss();
					}
					Ok(PlanOutcome::Skipped)
				},
			}
		} */
		Ok(PlanOutcome::Skipped)
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
}