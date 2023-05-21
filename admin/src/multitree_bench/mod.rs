// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use super::*;

mod data;

use parity_db::{Operation, NewNode};
pub use parity_db::{CompressionType, Db, Key, Value};

use rand::{RngCore, SeedableRng};
use std::{
	sync::{
		atomic::{AtomicBool, AtomicUsize, Ordering},
		Arc,
	},
	thread,
};

static COMMITS: AtomicUsize = AtomicUsize::new(0);
static NEXT_COMMIT: AtomicUsize = AtomicUsize::new(0);

//const COMMIT_SIZE: usize = 100;

/// Stress tests (warning erase db first).
#[derive(Debug, clap::Parser)]
pub struct MultiTreeStress {
	#[clap(flatten)]
	pub shared: Shared,

	/// Number of reading threads [default: 0].
	#[clap(long)]
	pub readers: Option<usize>,

	/// Number of iterating threads [default: 0].
	#[clap(long)]
	pub iter: Option<usize>,

	/// Number of writing threads [default: 1].
	#[clap(long)]
	pub writers: Option<usize>,

	/// Total number of inserted commits.
	#[clap(long)]
	pub commits: Option<usize>,

	/// Random seed used for key generation.
	#[clap(long)]
	pub seed: Option<u64>,

	/// Open an existing database.
	#[clap(long)]
	pub append: bool,

	/// Do not apply pruning.
	#[clap(long)]
	pub archive: bool,

	/// Enable compression.
	#[clap(long)]
	pub compress: bool,

	/// Time (in milliseconds) between commits.
	#[clap(long)]
	pub commit_time: Option<u64>,
}

#[derive(Clone)]
pub struct Args {
	pub readers: usize,
	pub iter: usize,
	pub writers: usize,
	pub commits: usize,
	pub seed: Option<u64>,
	pub append: bool,
	pub archive: bool,
	pub compress: bool,
	pub commit_time: u64,
}

impl MultiTreeStress {
	pub(super) fn get_args(&self) -> Args {
		Args {
			readers: self.readers.unwrap_or(0),
			iter: self.iter.unwrap_or(0),
			writers: self.writers.unwrap_or(1),
			commits: self.commits.unwrap_or(100_000),
			seed: self.seed,
			append: self.append,
			archive: self.archive,
			compress: self.compress,
			commit_time: self.commit_time.unwrap_or(0),
		}
	}
}

fn informant(shutdown: Arc<AtomicBool>) {
	while !shutdown.load(Ordering::Relaxed) {
		thread::sleep(std::time::Duration::from_secs(1));
	}
}

fn writer(
	db: Arc<Db>,
	args: Arc<Args>,
	shutdown: Arc<AtomicBool>,
	start_commit: usize,
) {
	let mut commit = Vec::new();

	loop {
		let n = NEXT_COMMIT.fetch_add(1, Ordering::SeqCst);
		if n >= start_commit + args.commits || shutdown.load(Ordering::Relaxed) {
			break
		}

		commit.push(Operation::InsertTree(vec![3u8,6,2,7,4,34,7,8,], NewNode { data: vec![57u8,1,7,8,34,5], children: Vec::new() }));

		db.commit_changes(commit);
		COMMITS.fetch_add(1, Ordering::Relaxed);
		commit.clear();
	}
}

fn reader(
	db: Arc<Db>,
	args: Arc<Args>,
	index: u64,
	shutdown: Arc<AtomicBool>,
) {
	// Query random keys while writing
	let seed = args.seed.unwrap_or(0);
	let mut rng = rand::rngs::SmallRng::seed_from_u64(seed + index);
	while !shutdown.load(Ordering::Relaxed) {
	}
}

fn iter(db: Arc<Db>, shutdown: Arc<AtomicBool>) {
	while !shutdown.load(Ordering::Relaxed) {
	}
}

pub fn run_internal(args: Args, db: Db) {
	let args = Arc::new(args);
	let shutdown = Arc::new(AtomicBool::new(false));
	let db = Arc::new(db);

	let mut threads = Vec::new();

	let start_commit = 0;

	let start_time = std::time::Instant::now();

	COMMITS.store(start_commit, Ordering::SeqCst);
	NEXT_COMMIT.store(start_commit, Ordering::SeqCst);

	{
		let shutdown = shutdown.clone();
		threads.push(thread::spawn(move || informant(shutdown)));
	}

	for i in 0..args.readers {
		let db = db.clone();
		let shutdown = shutdown.clone();
		let args = args.clone();

		threads.push(
			thread::Builder::new()
				.name(format!("reader {i}"))
				.spawn(move || reader(db, args, i as u64, shutdown))
				.unwrap(),
		);
	}

	for i in 0..args.iter {
		let db = db.clone();
		let shutdown = shutdown.clone();

		threads.push(
			thread::Builder::new()
				.name(format!("iter {i}"))
				.spawn(move || iter(db, shutdown))
				.unwrap(),
		);
	}

	for i in 0..args.writers {
		let db = db.clone();
		let shutdown = shutdown.clone();
		let args = args.clone();

		threads.push(
			thread::Builder::new()
				.name(format!("writer {i}"))
				.spawn(move || writer(db, args, shutdown, start_commit))
				.unwrap(),
		);
	}

	while COMMITS.load(Ordering::Relaxed) < start_commit + args.commits {
		thread::sleep(std::time::Duration::from_millis(50));
	}
	shutdown.store(true, Ordering::SeqCst);

	for t in threads.into_iter() {
		t.join().unwrap();
	}

	let commits = COMMITS.load(Ordering::SeqCst);
	let commits = commits - start_commit;
	let elapsed_time = start_time.elapsed().as_secs_f64();

	println!(
		"Completed {} commits in {} seconds.",
		commits,
		elapsed_time,
	);
}
