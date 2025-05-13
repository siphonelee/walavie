/// Module: walrusfs
module walrusfs::walrusfs;

use sui::tx_context::sender;
use sui::vec_map::{VecMap, Self};
use sui::vec_set::{VecSet, Self};
use sui::package;
use std::string::{Self, String};
use sui::clock::Clock;
use sui::event;

// the root object
public struct WalrusfsRoot has key, store {
	id: UID,
	current_epoch: u64,
	children_files: VecMap<String, u256>,
	children_directories: VecMap<String, u256>,

	// global variables
	obj_id: u256,
	file_arena: VecMap<u256, FileObject>,
	dir_arena: VecMap<u256, DirObject>,
}

public struct FileObject has copy, store, drop {
	create_ts: u64,
	tags: vector<String>,
	size: u64,
	walrus_blob_id: String,
	walrus_epoch_till: u64,
}

public struct DirObject has copy, store, drop {
	create_ts: u64,
	tags: vector<String>,
	children_files: VecMap<String, u256>,
	children_directories: VecMap<String, u256>,
}

public struct WALRUSFS has drop {}

const EPathError: u64 = 1;
const EArenaMismatchError: u64 = 2;
const EFileAlreadyExists: u64 = 3;
const EDirectoryAlreadyExists: u64 = 4;

public struct FileAlreadyExistsEvent has copy, drop {
	path: String,
	create_ts: u64,
	tags: vector<String>,
	size: u64,
	walrus_blob_id: String,
	walrus_epoch_till: u64,
}

public struct FileAddedEvent has copy, drop {
	path: String,
	create_ts: u64,
	tags: vector<String>,
	size: u64,
	walrus_blob_id: String,
	walrus_epoch_till: u64,
}

public struct DirAlreadyExistsEvent has copy, drop {
	path: String,
	create_ts: u64,
	tags: vector<String>,
}

public struct DirAddedEvent has copy, drop {
	path: String,
	create_ts: u64,
	tags: vector<String>,
}

public struct DirListObject has copy, store, drop {
	name: String,
	create_ts: u64,
	is_dir: bool,
	tags: vector<String>,
	size: u64,
	walrus_blob_id: String,
	walrus_epoch_till: u64,
}

public struct DeleteEvent has copy, drop {
	path: String,
}

public struct FileObjectEx has copy, store, drop {                                                
        id: u256, 
        obj: FileObject,
}       

public struct DirObjectEx has copy, store, drop {
        id: u256,      
        create_ts: u64,
        tags: vector<String>, 
        children_file_names: vector<String>,
        children_file_ids: vector<u256>,
        children_directory_names: vector<String>,
        children_directory_ids: vector<u256>,
}

public struct RecursiveDirList has copy, drop {
	dirobj: u256,
	files: vector<FileObjectEx>,
	dirs: vector<DirObjectEx>,
}

fun init(otw: WALRUSFS, ctx: &mut TxContext) {
	// Creating and sending the Publisher object to the sender.
	package::claim_and_keep(otw, ctx);

	// Creating and sending the WalrusfsRoot object to the sender.
	let root = WalrusfsRoot {
		id: object::new(ctx),
		current_epoch: 0,
		children_files: vec_map::empty(),
		children_directories: vec_map::empty(),
		obj_id: 0,
		file_arena: vec_map::empty(),
		dir_arena: vec_map::empty(),
	};

	transfer::transfer(root, ctx.sender());
}

public fun update_epoch(walrusfsRoot: &mut WalrusfsRoot, current_epoch: u64, _ctx: &mut TxContext) {
	walrusfsRoot.current_epoch = current_epoch;
}


public fun add_file(walrusfsRoot: &mut WalrusfsRoot, clock: &Clock, path: String, 
							tags: vector<String>, size: u64, 
							walrus_blob_id: String, end_epoch: u64,
							overwrite: bool, _ctx: &mut TxContext) {
	let mut p = path;
	let mut children = &walrusfsRoot.children_directories;
	let mut child_id = 0u256;
	while (true) {
		let idx = p.index_of(&b"/".to_string());
		let len = p.length();
		if (idx == len) {
			// the end
			break
		} else if (idx == 0) {
			// ignore trailing "/"
			p = p.substring(1, len);
		} else {
			let subp = p.substring(0, idx);
			// find the matching path
			assert!(children.contains(&subp), EPathError);

			child_id = *(children.get(&subp));
			assert!(walrusfsRoot.dir_arena.contains(&child_id), EArenaMismatchError);
			children = &walrusfsRoot.dir_arena.get(&child_id).children_directories;
			p = p.substring(idx + 1, len);
		}		
	};

	// TODO: same file/directory name allowed?

	assert!(p.length() > 0, EPathError);

	let mut children_files = &mut walrusfsRoot.children_files;
	if (child_id != 0) {
		children_files = &mut walrusfsRoot.dir_arena.get_mut(&child_id).children_files;
	};

	if (children_files.contains(&p)) {
		let id = children_files.get(&p);
		if (!overwrite) {
			let f = walrusfsRoot.file_arena.get(id);
			event::emit(FileAlreadyExistsEvent {
				path,
				create_ts: f.create_ts,
				tags: f.tags,
				size: f.size,
				walrus_blob_id: f.walrus_blob_id,
				walrus_epoch_till: f.walrus_epoch_till,
			});
			assert!(false, EFileAlreadyExists);
		} else {
			walrusfsRoot.file_arena.remove(id);
			children_files.remove(&p);
		};
	};

	// add file
	walrusfsRoot.obj_id = walrusfsRoot.obj_id + 1;
	let now = clock.timestamp_ms();
	vec_map::insert(&mut walrusfsRoot.file_arena, walrusfsRoot.obj_id, FileObject {
												create_ts: now,
												tags,
												size,
												walrus_blob_id,
												walrus_epoch_till: end_epoch,
											});
	vec_map::insert(children_files, p, walrusfsRoot.obj_id);
	event::emit(FileAddedEvent {
		path,
		create_ts: now,
		tags,
		size,
		walrus_blob_id,
		walrus_epoch_till: end_epoch,
	});
}

#[allow(unused_trailing_semi)]
public fun add_dir(walrusfsRoot: &mut WalrusfsRoot, clock: &Clock, path: String, 
							tags: vector<String>,
							_ctx: &mut TxContext) {
	let mut p = path;
	assert!(p.length() > 0, EPathError);

	let slash = b"/".to_string();
	// remove ending "/"
	if (p.substring(p.length() - 1, p.length()) == &slash) {
		p = p.substring(0, p.length() - 1);
	};

	let mut children = &mut walrusfsRoot.children_directories;

	while (true) {
		let idx = p.index_of(&slash);
		let len = p.length();

		if (idx == len) {
			// the end
			break;
		} else if (idx == 0) {
			// ignore trailing "/"
			p = p.substring(1, len);
		} else {
			let subp = p.substring(0, idx);
			// find the matching path
			assert!(children.contains(&subp), EPathError);

			let child_id = *(children.get(&subp));
			assert!(walrusfsRoot.dir_arena.contains(&child_id), EArenaMismatchError);
			children = &mut walrusfsRoot.dir_arena.get_mut(&child_id).children_directories;

			p = p.substring(idx + 1, len);
		}		
	};							

	// TODO: same file/directory name allowed?

	assert!(p.length() > 0, EPathError);
	if (children.contains(&p)) {
		let id = *children.get(&p);
		let d = walrusfsRoot.dir_arena.get(&id);
		event::emit(DirAlreadyExistsEvent {
			path,
			create_ts: d.create_ts,
			tags: d.tags,			
		});
		assert!(false, EDirectoryAlreadyExists);
		return;
	};

	// add dir
	walrusfsRoot.obj_id = walrusfsRoot.obj_id + 1;
	let now = clock.timestamp_ms();

	vec_map::insert(children, p, walrusfsRoot.obj_id);

	event::emit(DirAddedEvent {
		path,
		create_ts: now,
		tags,
	});

	walrusfsRoot.dir_arena.insert(walrusfsRoot.obj_id, DirObject {
												create_ts: now,
												tags,
												children_files: vec_map::empty(),
												children_directories: vec_map::empty(),
											});
											
}

public fun list_dir(walrusfsRoot: &WalrusfsRoot, path: String, _ctx: &mut TxContext): vector<DirListObject> {
	let mut p = path;
	assert!(p.length() > 0, EPathError);
	let slash = b"/".to_string();
	if (p.substring(p.length() - 1, p.length()) != &slash) {
		p.append(slash);
	};

	let mut children = &walrusfsRoot.children_directories;
	let mut children_files = &walrusfsRoot.children_files;
	while (true) {
		let idx = p.index_of(&slash);
		let len = p.length();
		if (idx == len) {
			// the end
			break
		} else if (idx == 0) {
			// ignore trailing "/"
			p = p.substring(1, len);
		} else {
			let subp = p.substring(0, idx);
			// find the matching path
			assert!(children.contains(&subp), EPathError);

			let child_id = *children.get(&subp);
			assert!(walrusfsRoot.dir_arena.contains(&child_id), EArenaMismatchError);
			children_files = &walrusfsRoot.dir_arena.get(&child_id).children_files;
			children = &walrusfsRoot.dir_arena.get(&child_id).children_directories;

			p = p.substring(idx + 1, len);
		}		
	};							

	let mut v: vector<DirListObject> = vector::empty();

	let mut i = 0;
	while (i < children.size()) {
		let (key, entry) = children.get_entry_by_idx(i);
		assert!(walrusfsRoot.dir_arena.contains(entry), EArenaMismatchError);
		let d = walrusfsRoot.dir_arena.get(entry);
		v.push_back(DirListObject {
			name: *key,
			create_ts: d.create_ts,
			is_dir: true,
			tags: d.tags,
			size: 0u64,
			walrus_blob_id: b"".to_string(),
			walrus_epoch_till: 0u64,
		});

		i = i + 1;
	};
	
	i = 0;
	while (i < children_files.size()) {
		let (key, entry) = children_files.get_entry_by_idx(i);
		assert!(walrusfsRoot.file_arena.contains(entry), EArenaMismatchError);
		let f = walrusfsRoot.file_arena.get(entry);
		v.push_back(DirListObject {
			name: *key,
			create_ts: f.create_ts,
			is_dir: false,
			tags: f.tags,
			size: f.size,
			walrus_blob_id: f.walrus_blob_id,
			walrus_epoch_till: f.walrus_epoch_till,
		});

		i = i + 1;
	};


        v
}

public fun stat(walrusfsRoot: &WalrusfsRoot, path: String, _ctx: &mut TxContext): DirListObject {
	let mut p = path;
	assert!(p.length() > 0, EPathError);

	let slash = b"/".to_string();
	// remove ending "/"
	if (p.substring(p.length() - 1, p.length()) == &slash) {
		p = p.substring(0, p.length() - 1);
	};

	let mut children = &walrusfsRoot.children_directories;
	let mut children_files = &walrusfsRoot.children_files;
	while (true) {
		let idx = p.index_of(&slash);
		let len = p.length();

		if (idx == len) {
			// can't be here
			break
		} else if (idx == 0) {
			// ignore trailing "/"
			p = p.substring(1, len);
		} else {
			let subp = p.substring(0, idx);
			// find the matching path
			assert!(children.contains(&subp), EPathError);

			let child_id = *children.get(&subp);
			assert!(walrusfsRoot.dir_arena.contains(&child_id), EArenaMismatchError);
			children_files = &walrusfsRoot.dir_arena.get(&child_id).children_files;
			children = &walrusfsRoot.dir_arena.get(&child_id).children_directories;

			p = p.substring(idx + 1, len);
		}		
	};		

	assert!(p.length() > 0, EPathError);

	if (children_files.contains(&p)) {
		let id = *children_files.get(&p);
		let f = walrusfsRoot.file_arena.get(&id);

		DirListObject {
			name: p,
			create_ts: f.create_ts,
			is_dir: false,
			tags: f.tags,
			size: f.size,
			walrus_blob_id: f.walrus_blob_id,
			walrus_epoch_till: f.walrus_epoch_till,
		}
	} else if (children.contains(&p)) {
		let id = *children.get(&p);
		let d = walrusfsRoot.dir_arena.get(&id);
		DirListObject {
			name: p,
			create_ts: d.create_ts,
			is_dir: true,
			tags: d.tags,
			size: 0u64,
			walrus_blob_id: b"".to_string(),
			walrus_epoch_till: 0u64,
		}
	} else {
		abort EPathError
	}
}

public fun rename_dir(walrusfsRoot: &mut WalrusfsRoot, frompath: String, topath: String, _ctx: &mut TxContext) {
	let mut from = frompath;
	let mut to = topath;
	assert!(from.length() > 0, EPathError);
	assert!(to.length() > 0, EPathError);

	let slash = b"/".to_string();
	// remove ending "/"
	if (from.substring(from.length() - 1, from.length()) == &slash) {
		from = from.substring(0, from.length() - 1);
	};
	if (to.substring(to.length() - 1, to.length()) == &slash) {
		to = to.substring(0, to.length() - 1);
	};

	while (to.substring(0, 1) == &slash) {
		to = to.substring(1, to.length());
	};

	let mut children = &mut walrusfsRoot.children_directories;
	while (true) {
		let idx = from.index_of(&slash);
		let len = from.length();

		if (idx == len) {
			// can't be here
			break
		} else if (idx == 0) {
			// ignore trailing "/"
			from = from.substring(1, len);
		} else {
			let subp = from.substring(0, idx);
			// find the matching path
			assert!(children.contains(&subp), EPathError);

			let len1 = to.length();
			let idx1 = to.index_of(&slash);
			assert!(idx1 != len1, EPathError);
			let subp1 = to.substring(0, idx1);
			assert!(subp == subp1, EPathError);
			to = to.substring(idx1 + 1, len1);

			let child_id = *children.get(&subp);
			assert!(walrusfsRoot.dir_arena.contains(&child_id), EArenaMismatchError);
			children = &mut walrusfsRoot.dir_arena.get_mut(&child_id).children_directories;

			from = from.substring(idx + 1, len);
		}		
	};	

	assert!(from.length() > 0, EPathError);
	assert!(to.length() > 0, EPathError);
	assert!(children.contains(&from), EPathError);
	assert!(!children.contains(&to), EDirectoryAlreadyExists);
	let id = *children.get(&from);
	children.insert(to, id);
	children.remove(&from);
}

public fun rename_file(walrusfsRoot: &mut WalrusfsRoot, frompath: String, topath: String, _ctx: &mut TxContext) {
	let mut from = frompath;
	let mut to = topath;
	assert!(from.length() > 0, EPathError);
	assert!(to.length() > 0, EPathError);

	let slash = b"/".to_string();
	// remove ending "/"
	if (from.substring(from.length() - 1, from.length()) == &slash) {
		from = from.substring(0, from.length() - 1);
	};
	if (to.substring(to.length() - 1, to.length()) == &slash) {
		to = to.substring(0, to.length() - 1);
	};

	while (to.substring(0, 1) == &slash) {
		to = to.substring(1, to.length());
	};

	let mut children = &walrusfsRoot.children_directories;
	let mut child_id = 0u256;
	while (true) {
		let idx = from.index_of(&slash);
		let len = from.length();

		if (idx == len) {
			// can't be here
			break
		} else if (idx == 0) {
			// ignore trailing "/"
			from = from.substring(1, len);
		} else {
			let subp = from.substring(0, idx);
			// find the matching path
			assert!(children.contains(&subp), EPathError);

			let len1 = to.length();
			let idx1 = to.index_of(&slash);
			assert!(idx1 != len1, EPathError);
			let subp1 = to.substring(0, idx1);
			assert!(subp == subp1, EPathError);
			to = to.substring(idx1 + 1, len1);

			child_id = *children.get(&subp);
			assert!(walrusfsRoot.dir_arena.contains(&child_id), EArenaMismatchError);
			children = &walrusfsRoot.dir_arena.get(&child_id).children_directories;

			from = from.substring(idx + 1, len);
		}		
	};	
	assert!(from.length() > 0, EPathError);
	assert!(to.length() > 0, EPathError);
	
	let mut children_files = &mut walrusfsRoot.children_files;
	if (child_id != 0) {
		children_files = &mut walrusfsRoot.dir_arena.get_mut(&child_id).children_files;
	};

	assert!(children_files.contains(&from), EPathError);
	assert!(!children_files.contains(&to), EDirectoryAlreadyExists);
	let id = *children_files.get(&from);
	children_files.insert(to, id);
	children_files.remove(&from);
}

public fun delete_file(walrusfsRoot: &mut WalrusfsRoot, path: String, _ctx: &mut TxContext) {
	let mut p = path;
	assert!(p.length() > 0, EPathError);

	let slash = b"/".to_string();
	// remove ending "/"
	if (p.substring(p.length() - 1, p.length()) == &slash) {
		p = p.substring(0, p.length() - 1);
	};

	let mut children = &walrusfsRoot.children_directories;
	let mut child_id = 0u256;
	while (true) {
		let idx = p.index_of(&slash);
		let len = p.length();

		if (idx == len) {
			// can't be here
			break
		} else if (idx == 0) {
			// ignore trailing "/"
			p = p.substring(1, len);
		} else {
			let subp = p.substring(0, idx);
			// find the matching path
			assert!(children.contains(&subp), EPathError);

			child_id = *children.get(&subp);
			assert!(walrusfsRoot.dir_arena.contains(&child_id), EArenaMismatchError);
			children = &walrusfsRoot.dir_arena.get(&child_id).children_directories;

			p = p.substring(idx + 1, len);
		}		
	};	
	assert!(p.length() > 0, EPathError);

	let mut children_files = &mut walrusfsRoot.children_files;
	if (child_id != 0) {
		children_files = &mut walrusfsRoot.dir_arena.get_mut(&child_id).children_files;
	};

	assert!(children_files.contains(&p), EPathError);

	let id = *children_files.get(&p);
	walrusfsRoot.file_arena.remove(&id);
	children_files.remove(&p);
}

fun recursive_get_dir_objs(walrusfsRoot: &WalrusfsRoot, id: u256): (VecSet<u256>, VecSet<u256>) {
	let mut f_set: VecSet<u256> = vec_set::empty();
	let mut d_set: VecSet<u256> = vec_set::empty();

	// collect sub-files
	let children_files = &walrusfsRoot.dir_arena.get(&id).children_files;
	let mut i = 0;
	while (i < children_files.size()) {
		let (_, fid) = children_files.get_entry_by_idx(i);
		i = i + 1;
		f_set.insert(*fid);
	};
	
    // collect sub-dirs
	let children = &walrusfsRoot.dir_arena.get(&id).children_directories;
	i = 0;
	while (i < children.size()) {
		let (_, did) = children.get_entry_by_idx(i);
		i = i + 1;

		let (fs, ds) = recursive_get_dir_objs(walrusfsRoot, *did);
		
		let mut fsv = fs.into_keys();
		while (!fsv.is_empty()) {
			f_set.insert(fsv.pop_back());
		};
		let mut dsv = ds.into_keys();
		while (!dsv.is_empty()) {
			d_set.insert(dsv.pop_back());
		};
	};

	d_set.insert(id);
	
	(f_set, d_set)
}

public fun delete_dir(walrusfsRoot: &mut WalrusfsRoot, path: String, _ctx: &mut TxContext) {
	let mut p = path;
	assert!(p.length() > 0, EPathError);

	let slash = b"/".to_string();
	// remove ending "/"
	if (p.substring(p.length() - 1, p.length()) == &slash) {
		p = p.substring(0, p.length() - 1);
	};

	let mut children = &mut walrusfsRoot.children_directories;
	let mut child_id ;
	while (true) {
		let idx = p.index_of(&slash);
		let len = p.length();

		if (idx == len) {
			// can't be here
			break
		} else if (idx == 0) {
			// ignore trailing "/"
			p = p.substring(1, len);
		} else {
			let subp = p.substring(0, idx);
			// find the matching path
			assert!(children.contains(&subp), EPathError);

			child_id = *children.get(&subp);
			assert!(walrusfsRoot.dir_arena.contains(&child_id), EArenaMismatchError);
			children = &mut walrusfsRoot.dir_arena.get_mut(&child_id).children_directories;

			p = p.substring(idx + 1, len);
		}		
	};	
	assert!(p.length() > 0, EPathError);

	assert!(children.contains(&p), EPathError);
	let id = *children.get(&p);
	children.remove(&p);

	let (fset, dset) = recursive_get_dir_objs(walrusfsRoot, id);

	let mut fsv = fset.into_keys();
	while (!fsv.is_empty()) {
		let fid = fsv.pop_back();
		walrusfsRoot.file_arena.remove(&fid);
	};

	let mut dsv = dset.into_keys();
	while (!dsv.is_empty()) {
		let did = dsv.pop_back();
		walrusfsRoot.dir_arena.remove(&did);
	};

	event::emit(DeleteEvent {
		path,
	});
}


public fun get_dir_all(walrusfsRoot: &WalrusfsRoot, path: String, _ctx: &mut TxContext): RecursiveDirList {
	let mut p = path;
	assert!(p.length() > 0, EPathError);

	let slash = b"/".to_string();
	// remove ending "/"
	if (p.substring(p.length() - 1, p.length()) == &slash) {
		p = p.substring(0, p.length() - 1);
	};

	let mut children = &walrusfsRoot.children_directories;
	let mut child_id;
	while (true) {
		let idx = p.index_of(&slash);
		let len = p.length();

		if (idx == len) {
			// can't be here
			break
		} else if (idx == 0) {
			// ignore trailing "/"
			p = p.substring(1, len);
		} else {
			let subp = p.substring(0, idx);
			// find the matching path
			assert!(children.contains(&subp), EPathError);

			child_id = *children.get(&subp);
			assert!(walrusfsRoot.dir_arena.contains(&child_id), EArenaMismatchError);
			children = &walrusfsRoot.dir_arena.get(&child_id).children_directories;

			p = p.substring(idx + 1, len);
		}		
	};	
	assert!(p.length() > 0, EPathError);

	assert!(children.contains(&p), EPathError);
	let id = *children.get(&p);

	let (fset, dset) = recursive_get_dir_objs(walrusfsRoot, id);

	let mut fsv = fset.into_keys();
	let mut files: vector<FileObjectEx> = vector::empty();
	while (!fsv.is_empty()) {
		let fid = fsv.pop_back();
		files.push_back(FileObjectEx {
                    id: fid, 
                    obj: *walrusfsRoot.file_arena.get(&fid)
                });
	};

	let mut dsv = dset.into_keys();
	let mut dirs: vector<DirObjectEx> = vector::empty();
	while (!dsv.is_empty()) {
		let did = dsv.pop_back(); 
                let do = *walrusfsRoot.dir_arena.get(&did);
                
                let mut cfns: vector<String> = vector::empty();
                let mut cfis: vector<u256> = vector::empty();
                
                let mut i = 0;
                while (i < do.children_files.size()) {         
                    let (k, v) = do.children_files.get_entry_by_idx(i);
                    cfns.push_back(*k);
                    cfis.push_back(*v);
                    i = i + 1;
                };

                let mut cdns: vector<String> = vector::empty();
                let mut cdis: vector<u256> = vector::empty();

                i = 0;
                while (i < do.children_directories.size()) {
                    let (k, v) = do.children_directories.get_entry_by_idx(i);
                    cdns.push_back(*k);
                    cdis.push_back(*v);
                    i = i + 1;
                };
                         
		dirs.push_back(DirObjectEx {
                     id: did, 
                     create_ts: do.create_ts,
                     tags: do.tags,
                     children_file_names: cfns,
                     children_file_ids: cfis,
                     children_directory_names: cdns,
                     children_directory_ids: cdis,
                });
	};

	RecursiveDirList {
		dirobj: id,
		files: files,
		dirs: dirs,
	}
}
