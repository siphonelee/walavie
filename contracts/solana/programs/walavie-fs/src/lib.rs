#![allow(unexpected_cfgs)]

// lib.rs
use anchor_lang::prelude::*;
use std::collections::BTreeSet; // BTreeSet is still used and generally fine

declare_id!("9NhNPHXjiCoZ9Hi5ch26x1yQJUq3u2weNoMeViwu7r2r"); // Replace with your program ID

// Max length for strings to manage account space, adjust as needed
const MAX_STRING_LEN: usize = 64;
const MAX_TAGS: usize = 5;

// Estimated space for PDAs (you'll need to manage realloc for production)
const WALRUSFS_ROOT_PDA_SPACE: usize = 8 + 8 + 8 + 32 + 1; // current_epoch + obj_id_counter + authority + bump
const CHILDREN_PDA_SPACE: usize = 1024 * 1; // For RootChildrenFiles/Dirs Pda (now Vec<KeyValueStringU64>)
const ARENA_PDA_SPACE: usize = 1024 * 1; // For File/Dir Arena Pda (now Vec<KeyValueU64Object>)
                                         // --- KeyValue Struct Definitions ---
#[derive(AnchorSerialize, AnchorDeserialize, Clone, Debug, PartialEq)]
pub struct KeyValueStringU64 {
    pub key: String,
    pub value: u64,
}

#[derive(AnchorSerialize, AnchorDeserialize, Clone, Debug)]
pub struct KeyValueU64FileObject {
    pub key: u64,
    pub value: FileObjectAnchor,
}

#[derive(AnchorSerialize, AnchorDeserialize, Clone, Debug)]
pub struct KeyValueU64DirObject {
    pub key: u64,
    pub value: DirObjectAnchor,
}

// --- PDA Struct Definitions (Modified) ---
#[account]
pub struct WalrusfsRootPda {
    pub current_epoch: u64,
    pub obj_id_counter: u64,
    pub authority: Pubkey,
    pub bump: u8,
}

#[account]
pub struct ChildrenFilesPda {
    pub data: Vec<KeyValueStringU64>, // Changed from BTreeMap
    pub bump: u8,
}

#[account]
pub struct ChildrenDirectoriesPda {
    pub data: Vec<KeyValueStringU64>, // Changed from BTreeMap
    pub bump: u8,
}

#[account]
pub struct FileArenaPda {
    pub data: Vec<KeyValueU64FileObject>, // Changed from BTreeMap
    pub bump: u8,
}

#[account]
pub struct DirArenaPda {
    pub data: Vec<KeyValueU64DirObject>, // Changed from BTreeMap
    pub bump: u8,
}

// --- Data Structs (DirObjectAnchor Modified) ---
#[derive(AnchorSerialize, AnchorDeserialize, Clone, Debug)]
pub struct FileObjectAnchor {
    pub create_ts: u64,
    pub tags: Vec<String>,
    pub size: u64,
    pub walrus_blob_id: String,
    pub walrus_epoch_till: u64,
}

#[derive(AnchorSerialize, AnchorDeserialize, Clone, Debug)]
pub struct DirObjectAnchor {
    pub create_ts: u64,
    pub tags: Vec<String>,
    pub children_files: Vec<KeyValueStringU64>, // Changed
    pub children_directories: Vec<KeyValueStringU64>, // Changed
}

// --- Helper Functions for Vec<KeyValue...> operations ---
// For Vec<KeyValueStringU64>
fn get_from_vec_str_key<'a>(vec: &'a [KeyValueStringU64], key: &str) -> Option<&'a u64> {
    vec.iter().find(|kv| kv.key == key).map(|kv| &kv.value)
}

fn get_mut_from_vec_str_key<'a>(
    vec: &'a mut [KeyValueStringU64],
    key: &'a str,
) -> Option<&'a mut u64> {
    vec.iter_mut()
        .find(|kv| kv.key == key)
        .map(|kv| &mut kv.value)
}

fn insert_into_vec_str_key(
    vec: &mut Vec<KeyValueStringU64>,
    key: String,
    value: u64,
) -> Option<u64> {
    if let Some(index) = vec.iter().position(|kv| kv.key == key) {
        let old_value = vec[index].value;
        vec[index].value = value;
        Some(old_value)
    } else {
        vec.push(KeyValueStringU64 { key, value });
        None
    }
}

fn remove_from_vec_str_key(vec: &mut Vec<KeyValueStringU64>, key: &str) -> Option<u64> {
    if let Some(index) = vec.iter().position(|kv| kv.key == key) {
        Some(vec.remove(index).value)
    } else {
        None
    }
}

fn contains_key_in_vec_str(vec: &[KeyValueStringU64], key: &str) -> bool {
    vec.iter().any(|kv| kv.key == key)
}

// For Vec<KeyValueU64FileObject> (File Arena)
fn get_from_file_arena<'a>(
    arena: &'a [KeyValueU64FileObject],
    id: u64,
) -> Option<&'a FileObjectAnchor> {
    arena.iter().find(|kv| kv.key == id).map(|kv| &kv.value)
}

// fn get_mut_from_file_arena(arena: &mut [KeyValueU64FileObject], id: &u64) -> Option<&mut FileObjectAnchor> {
//     arena.iter_mut().find(|kv| kv.key == *id).map(|kv| &mut kv.value)
// }

fn insert_into_file_arena(
    arena: &mut Vec<KeyValueU64FileObject>,
    id: u64,
    file_obj: FileObjectAnchor,
) -> Option<FileObjectAnchor> {
    if let Some(index) = arena.iter().position(|kv| kv.key == id) {
        let old_obj = std::mem::replace(&mut arena[index].value, file_obj);
        Some(old_obj)
    } else {
        arena.push(KeyValueU64FileObject {
            key: id,
            value: file_obj,
        });
        None
    }
}

fn remove_from_file_arena(
    arena: &mut Vec<KeyValueU64FileObject>,
    id: &u64,
) -> Option<FileObjectAnchor> {
    if let Some(index) = arena.iter().position(|kv| kv.key == *id) {
        Some(arena.remove(index).value)
    } else {
        None
    }
}

// For Vec<KeyValueU64DirObject> (Dir Arena)
fn get_from_dir_arena<'a>(
    arena: &'a [KeyValueU64DirObject],
    id: u64,
) -> Option<&'a DirObjectAnchor> {
    arena.iter().find(|kv| kv.key == id).map(|kv| &kv.value)
}

fn get_mut_from_dir_arena<'a>(
    arena: &'a mut [KeyValueU64DirObject],
    id: u64,
) -> Option<&'a mut DirObjectAnchor> {
    arena
        .iter_mut()
        .find(|kv| kv.key == id)
        .map(|kv| &mut kv.value)
}

fn insert_into_dir_arena(
    arena: &mut Vec<KeyValueU64DirObject>,
    id: u64,
    dir_obj: DirObjectAnchor,
) -> Option<DirObjectAnchor> {
    if let Some(index) = arena.iter().position(|kv| kv.key == id) {
        let old_obj = std::mem::replace(&mut arena[index].value, dir_obj);
        Some(old_obj)
    } else {
        arena.push(KeyValueU64DirObject {
            key: id,
            value: dir_obj,
        });
        None
    }
}

fn remove_from_dir_arena(
    arena: &mut Vec<KeyValueU64DirObject>,
    id: &u64,
) -> Option<DirObjectAnchor> {
    if let Some(index) = arena.iter().position(|kv| kv.key == *id) {
        Some(arena.remove(index).value)
    } else {
        None
    }
}

#[program]
pub mod walrusfs_anchor {
    use super::*;

    #[inline(never)]
    pub fn initialize_walrusfs(ctx: Context<InitializeWalrusfs>) -> Result<()> {
        let root = &mut ctx.accounts.walrusfs_root;
        root.current_epoch = 0;
        root.obj_id_counter = 0;
        root.authority = *ctx.accounts.payer.key;
        root.bump = ctx.bumps.walrusfs_root;

        let root_children_files = &mut ctx.accounts.root_children_files;
        root_children_files.data = Vec::new(); // Changed
        root_children_files.bump = ctx.bumps.root_children_files;

        let root_children_directories = &mut ctx.accounts.root_children_directories;
        root_children_directories.data = Vec::new(); // Changed
        root_children_directories.bump = ctx.bumps.root_children_directories;

        let file_arena = &mut ctx.accounts.file_arena;
        file_arena.data = Vec::new(); // Changed
        file_arena.bump = ctx.bumps.file_arena;

        let dir_arena = &mut ctx.accounts.dir_arena;
        dir_arena.data = Vec::new(); // Changed
        dir_arena.bump = ctx.bumps.dir_arena;

        Ok(())
    }

    pub fn update_epoch(ctx: Context<UpdateEpoch>, current_epoch: u64) -> Result<()> {
        require_keys_eq!(
            ctx.accounts.walrusfs_root.authority,
            ctx.accounts.authority.key(),
            WalrusFsError::Unauthorized
        );
        ctx.accounts.walrusfs_root.current_epoch = current_epoch;
        Ok(())
    }

    pub fn add_file(
        ctx: Context<AddFile>,
        path: String,
        tags: Vec<String>,
        size: u64,
        walrus_blob_id: String,
        end_epoch: u64,
        overwrite: bool,
    ) -> Result<()> {
        validate_path(&path)?;
        validate_tags(&tags)?;
        validate_string_len(&walrus_blob_id, "walrus_blob_id")?;

        let clock = Clock::get()?;
        let root = &mut ctx.accounts.walrusfs_root;
        let file_arena_data = &mut ctx.accounts.file_arena.data;
        let dir_arena_data_mut = &mut ctx.accounts.dir_arena.data;
        let root_children_files_data = &mut ctx.accounts.root_children_files.data;
        let root_children_dirs_data_ro = &ctx.accounts.root_children_directories.data;

        let (parent_dir_id, file_name) = internal_resolve_parent_id_and_name(
            &path,
            root_children_dirs_data_ro,
            dir_arena_data_mut,
        )?;

        let children_files_map: &mut Vec<KeyValueStringU64> = match parent_dir_id {
            Some(id) => {
                let parent_dir = get_mut_from_dir_arena(dir_arena_data_mut, id)
                    .ok_or(WalrusFsError::ArenaMismatchError)?;
                &mut parent_dir.children_files
            }
            None => root_children_files_data,
        };

        if let Some(existing_file_id) = get_from_vec_str_key(children_files_map, &file_name) {
            if !overwrite {
                let f = get_from_file_arena(file_arena_data, *existing_file_id)
                    .ok_or(WalrusFsError::ArenaMismatchError)?;
                emit!(FileAlreadyExistsEvent {
                    path: path.clone(),
                    create_ts: f.create_ts,
                    tags: f.tags.clone(),
                    size: f.size,
                    walrus_blob_id: f.walrus_blob_id.clone(),
                    walrus_epoch_till: f.walrus_epoch_till,
                });
                return err!(WalrusFsError::FileAlreadyExists);
            } else {
                // Remove from arena, id will be removed from children_files_map by insert_into_vec_str_key later
                remove_from_file_arena(file_arena_data, &existing_file_id);
                // Also explicitly remove from children_files_map before re-inserting if overwrite means true replacement.
                // However, insert_into_vec_str_key will update the value, which is what we want for the ID.
                // The key (file_name) remains, value (ID) changes.
            }
        }

        root.obj_id_counter += 1;
        let new_file_id = root.obj_id_counter;
        let now = clock.unix_timestamp as u64 * 1000;

        let new_file = FileObjectAnchor {
            create_ts: now,
            tags: tags.clone(),
            size,
            walrus_blob_id: walrus_blob_id.clone(),
            walrus_epoch_till: end_epoch,
        };
        insert_into_file_arena(file_arena_data, new_file_id, new_file);
        insert_into_vec_str_key(children_files_map, file_name.clone(), new_file_id);

        emit!(FileAddedEvent {
            path,
            create_ts: now,
            tags,
            size,
            walrus_blob_id,
            walrus_epoch_till: end_epoch,
        });

        Ok(())
    }

    pub fn add_dir(ctx: Context<AddDir>, path: String, tags: Vec<String>) -> Result<()> {
        let clean_path = remove_trailing_slash(&path);
        validate_path(&clean_path)?;
        validate_tags(&tags)?;

        let clock = Clock::get()?;
        let root = &mut ctx.accounts.walrusfs_root;
        let dir_arena_data = &mut ctx.accounts.dir_arena.data;
        let root_children_dirs_data = &mut ctx.accounts.root_children_directories.data;

        let (parent_dir_id, dir_name) = internal_resolve_parent_id_and_name(
            &clean_path,
            root_children_dirs_data,
            dir_arena_data,
        )?;

        let existing: Option<u64>;
        let children_dirs_map: &mut Vec<KeyValueStringU64> = match parent_dir_id {
            Some(id) => {
                let parent_dir = get_mut_from_dir_arena(dir_arena_data, id)
                    .ok_or(WalrusFsError::ArenaMismatchError)?;
                &mut parent_dir.children_directories
            }
            None => root_children_dirs_data,
        };

        existing = get_from_vec_str_key(children_dirs_map, &dir_name).copied();

        root.obj_id_counter += 1;
        let new_dir_id = root.obj_id_counter;
        insert_into_vec_str_key(children_dirs_map, dir_name.clone(), new_dir_id);

        if let Some(existing_dir_id) = existing {
            let d = get_from_dir_arena(dir_arena_data, existing_dir_id)
                .ok_or(WalrusFsError::ArenaMismatchError)?; // Should exist if ID is in children_dirs
            emit!(DirAlreadyExistsEvent {
                path: path.clone(),
                create_ts: d.create_ts,
                tags: d.tags.clone(),
            });
            return err!(WalrusFsError::DirectoryAlreadyExists);
        }

        let now = clock.unix_timestamp as u64 * 1000;
        let new_dir = DirObjectAnchor {
            create_ts: now,
            tags: tags.clone(),
            children_files: Vec::new(),       // Changed
            children_directories: Vec::new(), // Changed
        };
        insert_into_dir_arena(dir_arena_data, new_dir_id, new_dir);

        emit!(DirAddedEvent {
            path,
            create_ts: now,
            tags
        });
        Ok(())
    }

    pub fn list_dir(ctx: Context<ListDir>, path: String) -> Result<Vec<DirListObjectAnchor>> {
        let path_with_slash = ensure_trailing_slash(&path);
        validate_path(&path_with_slash)?;

        let file_arena_data = &ctx.accounts.file_arena.data;
        let dir_arena_data = &ctx.accounts.dir_arena.data;
        let root_children_files_data = &ctx.accounts.root_children_files.data;
        let root_children_dirs_data = &ctx.accounts.root_children_directories.data;

        let (target_dir_files_vec, target_dir_dirs_vec) = internal_get_dir_children_refs(
            &path_with_slash,
            root_children_files_data,
            root_children_dirs_data,
            dir_arena_data,
        )?;

        let mut results = Vec::new();

        for kv_pair in target_dir_dirs_vec.iter() {
            // Iterate over Vec<KeyValueStringU64>
            let d = get_from_dir_arena(dir_arena_data, kv_pair.value)
                .ok_or(WalrusFsError::ArenaMismatchError)?;
            results.push(DirListObjectAnchor {
                name: kv_pair.key.clone(),
                create_ts: d.create_ts,
                is_dir: true,
                tags: d.tags.clone(),
                size: 0,
                walrus_blob_id: String::new(),
                walrus_epoch_till: 0,
            });
        }

        for kv_pair in target_dir_files_vec.iter() {
            // Iterate over Vec<KeyValueStringU64>
            let f = get_from_file_arena(file_arena_data, kv_pair.value)
                .ok_or(WalrusFsError::ArenaMismatchError)?;
            results.push(DirListObjectAnchor {
                name: kv_pair.key.clone(),
                create_ts: f.create_ts,
                is_dir: false,
                tags: f.tags.clone(),
                size: f.size,
                walrus_blob_id: f.walrus_blob_id.clone(),
                walrus_epoch_till: f.walrus_epoch_till,
            });
        }
        Ok(results)
    }

    pub fn stat(ctx: Context<Stat>, path: String) -> Result<DirListObjectAnchor> {
        let clean_path = remove_trailing_slash(&path);
        validate_path(&clean_path)?;

        let file_arena_data = &ctx.accounts.file_arena.data;
        let dir_arena_data = &ctx.accounts.dir_arena.data;
        let root_children_files_data = &ctx.accounts.root_children_files.data;
        let root_children_dirs_data = &ctx.accounts.root_children_directories.data;

        let (parent_dir_id, item_name) = internal_resolve_parent_id_and_name(
            &clean_path,
            root_children_dirs_data,
            dir_arena_data,
        )?;

        let (parent_files_vec, parent_dirs_vec) = match parent_dir_id {
            Some(id) => {
                let parent_dir = get_from_dir_arena(dir_arena_data, id)
                    .ok_or(WalrusFsError::ArenaMismatchError)?;
                (&parent_dir.children_files, &parent_dir.children_directories)
            },
            None => {
                (root_children_files_data, root_children_dirs_data)
            },
        };

        if let Some(file_id_ref) = get_from_vec_str_key(parent_files_vec, &item_name) {
            let f = get_from_file_arena(file_arena_data, *file_id_ref)
                .ok_or(WalrusFsError::ArenaMismatchError)?;
            Ok(DirListObjectAnchor {
                name: item_name,
                create_ts: f.create_ts,
                is_dir: false,
                tags: f.tags.clone(),
                size: f.size,
                walrus_blob_id: f.walrus_blob_id.clone(),
                walrus_epoch_till: f.walrus_epoch_till,
            })
        } else if let Some(dir_id_ref) = get_from_vec_str_key(parent_dirs_vec, &item_name) {
            let d = get_from_dir_arena(dir_arena_data, *dir_id_ref)
                .ok_or(WalrusFsError::ArenaMismatchError)?;
            Ok(DirListObjectAnchor {
                name: item_name,
                create_ts: d.create_ts,
                is_dir: true,
                tags: d.tags.clone(),
                size: 0,
                walrus_blob_id: String::new(),
                walrus_epoch_till: 0,
            })
        } else {
            err!(WalrusFsError::PathNotFound)
        }
    }

    pub fn rename_file(ctx: Context<RenameFile>, from_path: String, to_path: String) -> Result<()> {
        let clean_from_path = remove_trailing_slash(&from_path);
        let clean_to_path = remove_trailing_slash(&to_path);
        validate_path(&clean_from_path)?;
        validate_path(&clean_to_path)?;

        let dir_arena_data = &mut ctx.accounts.dir_arena.data;
        let root_children_files_data = &mut ctx.accounts.root_children_files.data;
        let root_children_dirs_data_for_read = &ctx.accounts.root_children_directories.data;

        let (from_parent_id, from_name) = internal_resolve_parent_id_and_name(
            &clean_from_path,
            root_children_dirs_data_for_read,
            dir_arena_data,
        )?;
        let (to_parent_id, to_name) = internal_resolve_parent_id_and_name(
            &clean_to_path,
            root_children_dirs_data_for_read,
            dir_arena_data,
        )?;

        require!(
            from_parent_id == to_parent_id,
            WalrusFsError::RenamePathMismatch
        );

        let children_files_vec: &mut Vec<KeyValueStringU64> = match from_parent_id {
            Some(id) => {
                let parent_dir = get_mut_from_dir_arena(dir_arena_data, id)
                    .ok_or(WalrusFsError::ArenaMismatchError)?;
                &mut parent_dir.children_files
            }
            None => root_children_files_data,
        };

        require!(
            contains_key_in_vec_str(children_files_vec, &from_name),
            WalrusFsError::PathNotFound
        );
        require!(
            !contains_key_in_vec_str(children_files_vec, &to_name),
            WalrusFsError::FileAlreadyExists
        );

        let file_id = remove_from_vec_str_key(children_files_vec, &from_name).unwrap(); // Should exist due to check
        insert_into_vec_str_key(children_files_vec, to_name, file_id);

        Ok(())
    }

    pub fn rename_dir(ctx: Context<RenameDir>, from_path: String, to_path: String) -> Result<()> {
        let clean_from_path = remove_trailing_slash(&from_path);
        let clean_to_path = remove_trailing_slash(&to_path);
        validate_path(&clean_from_path)?;
        validate_path(&clean_to_path)?;

        let dir_arena_data = &mut ctx.accounts.dir_arena.data;
        let root_children_dirs_data = &mut ctx.accounts.root_children_directories.data;

        let (from_parent_id, from_name) = internal_resolve_parent_id_and_name(
            &clean_from_path,
            root_children_dirs_data,
            dir_arena_data,
        )?;
        let (to_parent_id, to_name) = internal_resolve_parent_id_and_name(
            &clean_to_path,
            root_children_dirs_data,
            dir_arena_data,
        )?;

        require!(
            from_parent_id == to_parent_id,
            WalrusFsError::RenamePathMismatch
        );

        let children_dirs_vec: &mut Vec<KeyValueStringU64> = match from_parent_id {
            Some(id) => {
                let parent_dir = get_mut_from_dir_arena(dir_arena_data, id)
                    .ok_or(WalrusFsError::ArenaMismatchError)?;
                &mut parent_dir.children_directories
            }
            None => root_children_dirs_data,
        };

        require!(
            contains_key_in_vec_str(children_dirs_vec, &from_name),
            WalrusFsError::PathNotFound
        );
        require!(
            !contains_key_in_vec_str(children_dirs_vec, &to_name),
            WalrusFsError::DirectoryAlreadyExists
        );

        let dir_id = remove_from_vec_str_key(children_dirs_vec, &from_name).unwrap();
        insert_into_vec_str_key(children_dirs_vec, to_name, dir_id);
        Ok(())
    }

    pub fn delete_file(ctx: Context<DeleteFile>, path: String) -> Result<()> {
        let clean_path = remove_trailing_slash(&path);
        validate_path(&clean_path)?;

        let file_arena_data = &mut ctx.accounts.file_arena.data;
        let dir_arena_data = &mut ctx.accounts.dir_arena.data;
        let root_children_files_data = &mut ctx.accounts.root_children_files.data;
        let root_children_dirs_data_ro = &ctx.accounts.root_children_directories.data;

        let (parent_dir_id, file_name) = internal_resolve_parent_id_and_name(
            &clean_path,
            root_children_dirs_data_ro,
            dir_arena_data,
        )?;

        let children_files_vec: &mut Vec<KeyValueStringU64> = match parent_dir_id {
            Some(id) => {
                let parent_dir = get_mut_from_dir_arena(dir_arena_data, id)
                    .ok_or(WalrusFsError::ArenaMismatchError)?;
                &mut parent_dir.children_files
            }
            None => root_children_files_data,
        };

        let file_id = remove_from_vec_str_key(children_files_vec, &file_name)
            .ok_or(WalrusFsError::PathNotFound)?;
        remove_from_file_arena(file_arena_data, &file_id)
            .ok_or(WalrusFsError::ArenaMismatchError)?; // Ensure it was in arena

        emit!(DeleteEvent { path });
        Ok(())
    }

    pub fn delete_dir(ctx: Context<DeleteDir>, path: String) -> Result<()> {
        let clean_path = remove_trailing_slash(&path);
        validate_path(&clean_path)?;

        let file_arena_data = &mut ctx.accounts.file_arena.data;
        let dir_arena_data = &mut ctx.accounts.dir_arena.data;
        let root_children_dirs_data = &mut ctx.accounts.root_children_directories.data;

        let (parent_dir_id, dir_name_to_delete) = internal_resolve_parent_id_and_name(
            &clean_path,
            root_children_dirs_data,
            dir_arena_data,
        )?;

        let dir_id_to_delete = {
            let children_dirs_vec: &mut Vec<KeyValueStringU64> = match parent_dir_id {
                Some(id) => {
                    let parent_dir = get_mut_from_dir_arena(dir_arena_data, id)
                        .ok_or(WalrusFsError::ArenaMismatchError)?;
                    &mut parent_dir.children_directories
                }
                None => root_children_dirs_data,
            };
            remove_from_vec_str_key(children_dirs_vec, &dir_name_to_delete)
                .ok_or(WalrusFsError::PathNotFound)?
        };

        let (files_to_delete, dirs_to_delete_recursive) =
            internal_recursive_get_dir_obj_ids(dir_id_to_delete, dir_arena_data)?;

        for file_id in files_to_delete {
            remove_from_file_arena(file_arena_data, &file_id); // .ok_or(WalrusFsError::ArenaMismatchError)?; // Optionally check, but might be gone
        }
        for dir_id in dirs_to_delete_recursive {
            remove_from_dir_arena(dir_arena_data, &dir_id); // .ok_or(WalrusFsError::ArenaMismatchError)?;
        }
        remove_from_dir_arena(dir_arena_data, &dir_id_to_delete); // .ok_or(WalrusFsError::ArenaMismatchError)?;

        emit!(DeleteEvent { path });
        Ok(())
    }

    pub fn get_dir_all(ctx: Context<GetDirAll>, path: String) -> Result<RecursiveDirListAnchor> {
        let clean_path = remove_trailing_slash(&path);
        validate_path(&clean_path)?;

        let file_arena_data = &ctx.accounts.file_arena.data;
        let dir_arena_data = &ctx.accounts.dir_arena.data;
        let root_children_dirs_data = &ctx.accounts.root_children_directories.data;

        let target_dir_id = {
            let (grandparent_dir_id, target_dir_name_from_parent) =
                internal_resolve_parent_id_and_name(
                    &clean_path,
                    root_children_dirs_data,
                    dir_arena_data,
                )?;

            let grandparent_children_dirs_vec = match grandparent_dir_id {
                Some(id) => {
                    &get_from_dir_arena(dir_arena_data, id)
                        .ok_or(WalrusFsError::ArenaMismatchError)?
                        .children_directories
                }
                None => root_children_dirs_data,
            };
            get_from_vec_str_key(grandparent_children_dirs_vec, &target_dir_name_from_parent)
                .ok_or(WalrusFsError::PathNotFound)?
        };

        let (file_ids, dir_ids_recursive) =
            internal_recursive_get_dir_obj_ids(*target_dir_id, dir_arena_data)?;

        let mut files_ex = Vec::new();
        for fid in file_ids {
            if let Some(obj) = get_from_file_arena(file_arena_data, fid) {
                files_ex.push(FileObjectExAnchor {
                    id: fid,
                    obj: obj.clone(),
                });
            }
        }

        let mut dirs_ex = Vec::new();
        let mut all_dir_ids_to_fetch = BTreeSet::new();
        all_dir_ids_to_fetch.insert(*target_dir_id);
        for did in dir_ids_recursive {
            all_dir_ids_to_fetch.insert(did);
        }

        for did in all_dir_ids_to_fetch {
            if let Some(d_obj) = get_from_dir_arena(dir_arena_data, did) {
                dirs_ex.push(DirObjectExAnchor {
                    id: did,
                    create_ts: d_obj.create_ts,
                    tags: d_obj.tags.clone(),
                    children_file_names: d_obj
                        .children_files
                        .iter()
                        .map(|kv| kv.key.clone())
                        .collect(),
                    children_file_ids: d_obj.children_files.iter().map(|kv| kv.value).collect(),
                    children_directory_names: d_obj
                        .children_directories
                        .iter()
                        .map(|kv| kv.key.clone())
                        .collect(),
                    children_directory_ids: d_obj
                        .children_directories
                        .iter()
                        .map(|kv| kv.value)
                        .collect(),
                });
            }
        }

        Ok(RecursiveDirListAnchor {
            dirobj: *target_dir_id,
            files: files_ex,
            dirs: dirs_ex,
        })
    }
}

// --- Internal Helper Functions (Modified parameters, core logic adapted) ---
fn internal_resolve_parent_id_and_name<'a>(
    full_path: &str,
    root_children_dirs_data: &'a Vec<KeyValueStringU64>,
    dir_arena_data: &'a Vec<KeyValueU64DirObject>,
) -> Result<(Option<u64>, String)> {
    let path = remove_trailing_slash(full_path);
    if path == "/" {
        return err!(WalrusFsError::InvalidPathOperationOnRoot);
    }

    let mut components = path
        .split('/')
        .filter(|s| !s.is_empty())
        .collect::<Vec<&str>>();
    if components.is_empty() {
        return err!(WalrusFsError::PathError);
    }

    let name = components.pop().unwrap().to_string();

    let mut current_parent_id: Option<u64> = None;
    let mut current_children_dirs_vec: &Vec<KeyValueStringU64> = root_children_dirs_data;

    for component_str in components {
        let component = component_str.to_string();
        let found_id_ref = get_from_vec_str_key(current_children_dirs_vec, &component)
            .ok_or(WalrusFsError::PathNotFound)?;

        current_parent_id = Some(*found_id_ref);
        let dir_object = get_from_dir_arena(dir_arena_data, *found_id_ref)
            .ok_or(WalrusFsError::ArenaMismatchError)?;
        current_children_dirs_vec = &dir_object.children_directories;
    }
    Ok((current_parent_id, name))
}

fn internal_get_dir_children_refs<'a>(
    path_with_trailing_slash: &str,
    root_children_files_data: &'a Vec<KeyValueStringU64>,
    root_children_dirs_data: &'a Vec<KeyValueStringU64>,
    dir_arena_data: &'a Vec<KeyValueU64DirObject>,
) -> Result<(Vec<KeyValueStringU64>, Vec<KeyValueStringU64>)> {
    if path_with_trailing_slash == "/" {
        return Ok((
            root_children_files_data.clone(),
            root_children_dirs_data.clone(),
        ));
    }

    let components: Vec<&str> = path_with_trailing_slash
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();

    let mut current_dir_id_opt: Option<u64> = None;
    let mut current_children_dirs_vec_ref = root_children_dirs_data;

    for component_str in components {
        let component = component_str.to_string();
        let dir_id_ref = get_from_vec_str_key(current_children_dirs_vec_ref, &component)
            .ok_or(WalrusFsError::PathNotFound)?;

        let dir_object = get_from_dir_arena(dir_arena_data, *dir_id_ref)
            .ok_or(WalrusFsError::ArenaMismatchError)?;

        current_children_dirs_vec_ref = &dir_object.children_directories;
        current_dir_id_opt = Some(*dir_id_ref);
    }

    let target_dir_id = current_dir_id_opt.ok_or(WalrusFsError::PathNotFound)?; // Should be Some if path is valid and not root
    let target_dir_obj = get_from_dir_arena(dir_arena_data, target_dir_id)
        .ok_or(WalrusFsError::ArenaMismatchError)?;

    Ok((
        target_dir_obj.children_files.clone(),
        target_dir_obj.children_directories.clone(),
    ))
}

fn internal_recursive_get_dir_obj_ids(
    dir_id: u64,
    dir_arena_data: &Vec<KeyValueU64DirObject>,
) -> Result<(BTreeSet<u64>, BTreeSet<u64>)> {
    let mut file_ids = BTreeSet::new();
    let mut dir_ids_recursive = BTreeSet::new();

    let mut dirs_to_process = vec![dir_id];
    let mut visited_dirs = BTreeSet::new();

    while let Some(current_dir_id) = dirs_to_process.pop() {
        if visited_dirs.contains(&current_dir_id) {
            continue;
        }
        visited_dirs.insert(current_dir_id);

        let dir_object = get_from_dir_arena(dir_arena_data, current_dir_id)
            .ok_or(WalrusFsError::ArenaMismatchError)?;

        for kv_pair in dir_object.children_files.iter() {
            file_ids.insert(kv_pair.value);
        }

        for kv_pair in dir_object.children_directories.iter() {
            let sub_dir_id = kv_pair.value;
            if sub_dir_id != dir_id {
                // Avoid self-reference cycles if they could exist (though unlikely here)
                dir_ids_recursive.insert(sub_dir_id);
            }
            // Only add to process if not already visited (implicitly handled by visited_dirs check at loop start)
            if !visited_dirs.contains(&sub_dir_id) {
                // Explicit check here can be slightly more efficient
                dirs_to_process.push(sub_dir_id);
            }
        }
    }
    Ok((file_ids, dir_ids_recursive))
}
// --- Path Validation and String Utils (Unchanged) ---
fn validate_path(path: &str) -> Result<()> {
    if path.is_empty() || path.len() > MAX_STRING_LEN * 5 {
        // Path can be multiple components
        return err!(WalrusFsError::PathError);
    }
    if !path.starts_with('/') && !path.is_empty() {
        return err!(WalrusFsError::PathError);
    }
    if path.contains("//") {
        return err!(WalrusFsError::PathError);
    }
    Ok(())
}

fn validate_tags(tags: &[String]) -> Result<()> {
    if tags.len() > MAX_TAGS {
        return err!(WalrusFsError::TooManyTags);
    }
    for tag in tags {
        validate_string_len(tag, "tag")?;
    }
    Ok(())
}

fn validate_string_len(s: &str, field_name: &str) -> Result<()> {
    if s.len() > MAX_STRING_LEN {
        msg!(
            "String too long for field: {}. Max: {}, Found: {}",
            field_name,
            MAX_STRING_LEN,
            s.len()
        );
        return err!(WalrusFsError::StringTooLong);
    }
    Ok(())
}

fn remove_trailing_slash(path: &str) -> String {
    if path != "/" && path.ends_with('/') {
        path[..path.len() - 1].to_string()
    } else {
        path.to_string()
    }
}

fn ensure_trailing_slash(path: &str) -> String {
    if path == "/" {
        return "/".to_string();
    } // Keep root as is
    if !path.ends_with('/') {
        format!("{}/", path)
    } else {
        path.to_string()
    }
}

// --- Accounts Structs for Instructions (Unchanged Structurally, but types inside accounts are modified) ---
// --- (All `#[derive(Accounts)]` structs remain as they were, definitions are not repeated for brevity) ---
// Example:
#[derive(Accounts)]
pub struct InitializeWalrusfs<'info> {
    #[account(
        init,
        payer = payer,
        space = WALRUSFS_ROOT_PDA_SPACE,
        seeds = [b"walrusfs_root".as_ref(), payer.key().as_ref()],
        bump
    )]
    pub walrusfs_root: Box<Account<'info, WalrusfsRootPda>>,
    #[account(
        init,
        payer = payer,
        space = CHILDREN_PDA_SPACE,
        seeds = [b"root_children_files".as_ref(), walrusfs_root.key().as_ref()],
        bump
    )]
    pub root_children_files: Box<Account<'info, ChildrenFilesPda>>, // Type inside uses Vec now
    #[account(
        init,
        payer = payer,
        space = CHILDREN_PDA_SPACE,
        seeds = [b"root_children_directories".as_ref(), walrusfs_root.key().as_ref()],
        bump
    )]
    pub root_children_directories: Box<Account<'info, ChildrenDirectoriesPda>>, // Type inside uses Vec
    #[account(
        init,
        payer = payer,
        space = ARENA_PDA_SPACE,
        seeds = [b"file_arena".as_ref(), walrusfs_root.key().as_ref()],
        bump
    )]
    pub file_arena: Box<Account<'info, FileArenaPda>>, // Type inside uses Vec
    #[account(
        init,
        payer = payer,
        space = ARENA_PDA_SPACE,
        seeds = [b"dir_arena".as_ref(), walrusfs_root.key().as_ref()],
        bump
    )]
    pub dir_arena: Box<Account<'info, DirArenaPda>>, // Type inside uses Vec
    #[account(mut)]
    pub payer: Signer<'info>,
    pub system_program: Program<'info, System>,
}

#[derive(Accounts)]
pub struct AddDir<'info> {
    pub authority: Signer<'info>,
    #[account(
        mut,
        seeds = [b"walrusfs_root".as_ref(), authority.key().as_ref()],
        bump = walrusfs_root.bump
    )]
    pub walrusfs_root: Box<Account<'info, WalrusfsRootPda>>,
    #[account( 
        mut, // Mutable if adding to root
        seeds = [b"root_children_directories".as_ref(), walrusfs_root.key().as_ref()],
        bump = root_children_directories.bump
    )]
    pub root_children_directories: Box<Account<'info, ChildrenDirectoriesPda>>,
    #[account(
        mut,
        seeds = [b"dir_arena".as_ref(), walrusfs_root.key().as_ref()],
        bump = dir_arena.bump
    )]
    pub dir_arena: Box<Account<'info, DirArenaPda>>,
}

#[derive(Accounts)]
pub struct RenameDir<'info> {
    pub authority: Signer<'info>,
    #[account(
        seeds = [b"walrusfs_root".as_ref(), authority.key().as_ref()],
        bump = walrusfs_root.bump
    )]
    pub walrusfs_root: Box<Account<'info, WalrusfsRootPda>>,
    #[account( 
        mut, // Could be renaming a dir in root, or a dir in a subdir (affecting dir_arena)
        seeds = [b"root_children_directories".as_ref(), walrusfs_root.key().as_ref()],
        bump = root_children_directories.bump
    )]
    pub root_children_directories: Box<Account<'info, ChildrenDirectoriesPda>>,
    #[account( 
        mut,
        seeds = [b"dir_arena".as_ref(), walrusfs_root.key().as_ref()],
        bump = dir_arena.bump
    )]
    pub dir_arena: Box<Account<'info, DirArenaPda>>,
}

// Specific read operations will use the ReadUserFileSystem context
#[derive(Accounts)]
pub struct ListDir<'info> {
    // Inherits structure from ReadUserFileSystem
    /// CHECK: Owner of the filesystem.
    pub owner: AccountInfo<'info>,
    #[account(seeds = [b"walrusfs_root".as_ref(), owner.key().as_ref()], bump = walrusfs_root.bump)]
    pub walrusfs_root: Box<Account<'info, WalrusfsRootPda>>,
    #[account(seeds = [b"root_children_files".as_ref(), walrusfs_root.key().as_ref()], bump = root_children_files.bump)]
    pub root_children_files: Box<Account<'info, ChildrenFilesPda>>,
    #[account(seeds = [b"root_children_directories".as_ref(), walrusfs_root.key().as_ref()], bump = root_children_directories.bump)]
    pub root_children_directories: Box<Account<'info, ChildrenDirectoriesPda>>,
    #[account(seeds = [b"file_arena".as_ref(), walrusfs_root.key().as_ref()], bump = file_arena.bump)]
    pub file_arena: Box<Account<'info, FileArenaPda>>,
    #[account(seeds = [b"dir_arena".as_ref(), walrusfs_root.key().as_ref()], bump = dir_arena.bump)]
    pub dir_arena: Box<Account<'info, DirArenaPda>>,
}

#[derive(Accounts)]
pub struct UpdateEpoch<'info> {
    pub authority: Signer<'info>, // The owner of this filesystem instance
    #[account(
        mut,
        seeds = [b"walrusfs_root".as_ref(), authority.key().as_ref()],
        bump = walrusfs_root.bump,
        // constraint = walrusfs_root.authority == authority.key() @ WalrusFsError::Unauthorized // Redundant due to seed but can be explicit
    )]
    pub walrusfs_root: Box<Account<'info, WalrusfsRootPda>>,
}

#[derive(Accounts)]
pub struct AddFile<'info> {
    pub authority: Signer<'info>,
    #[account(
        mut,
        seeds = [b"walrusfs_root".as_ref(), authority.key().as_ref()],
        bump = walrusfs_root.bump
    )]
    pub walrusfs_root: Box<Account<'info, WalrusfsRootPda>>,
    #[account(
        mut,
        seeds = [b"root_children_files".as_ref(), walrusfs_root.key().as_ref()],
        bump = root_children_files.bump
    )]
    pub root_children_files: Box<Account<'info, ChildrenFilesPda>>,
    #[account( // Read-only for traversal, but derived from user-specific root
        seeds = [b"root_children_directories".as_ref(), walrusfs_root.key().as_ref()],
        bump = root_children_directories.bump
    )]
    pub root_children_directories: Box<Account<'info, ChildrenDirectoriesPda>>,
    #[account(
        mut,
        seeds = [b"file_arena".as_ref(), walrusfs_root.key().as_ref()],
        bump = file_arena.bump
    )]
    pub file_arena: Box<Account<'info, FileArenaPda>>,
    #[account( // Mutable because a parent DirObject's children_files list might be updated
        mut, 
        seeds = [b"dir_arena".as_ref(), walrusfs_root.key().as_ref()],
        bump = dir_arena.bump
    )]
    pub dir_arena: Box<Account<'info, DirArenaPda>>,
}

#[derive(Accounts)]
pub struct Stat<'info> {
    // Inherits structure from ReadUserFileSystem
    /// CHECK: Owner of the filesystem.
    pub owner: AccountInfo<'info>,
    #[account(seeds = [b"walrusfs_root".as_ref(), owner.key().as_ref()], bump = walrusfs_root.bump)]
    pub walrusfs_root: Box<Account<'info, WalrusfsRootPda>>,
    #[account(seeds = [b"root_children_files".as_ref(), walrusfs_root.key().as_ref()], bump = root_children_files.bump)]
    pub root_children_files: Box<Account<'info, ChildrenFilesPda>>,
    #[account(seeds = [b"root_children_directories".as_ref(), walrusfs_root.key().as_ref()], bump = root_children_directories.bump)]
    pub root_children_directories: Box<Account<'info, ChildrenDirectoriesPda>>,
    #[account(seeds = [b"file_arena".as_ref(), walrusfs_root.key().as_ref()], bump = file_arena.bump)]
    pub file_arena: Box<Account<'info, FileArenaPda>>,
    #[account(seeds = [b"dir_arena".as_ref(), walrusfs_root.key().as_ref()], bump = dir_arena.bump)]
    pub dir_arena: Box<Account<'info, DirArenaPda>>,
}

#[derive(Accounts)]
pub struct RenameFile<'info> {
    pub authority: Signer<'info>,
    #[account(
        // Not mutable itself, but needed for deriving other PDA keys
        seeds = [b"walrusfs_root".as_ref(), authority.key().as_ref()],
        bump = walrusfs_root.bump
    )]
    pub walrusfs_root: Box<Account<'info, WalrusfsRootPda>>,
    #[account(
        mut, // Children list at root could change
        seeds = [b"root_children_files".as_ref(), walrusfs_root.key().as_ref()],
        bump = root_children_files.bump
    )]
    pub root_children_files: Box<Account<'info, ChildrenFilesPda>>,
    #[account( // For path traversal only
        seeds = [b"root_children_directories".as_ref(), walrusfs_root.key().as_ref()],
        bump = root_children_directories.bump
    )]
    pub root_children_directories: Box<Account<'info, ChildrenDirectoriesPda>>,
    #[account( // Dir arena is mutable as children_files within a DirObject might change
        mut,
        seeds = [b"dir_arena".as_ref(), walrusfs_root.key().as_ref()],
        bump = dir_arena.bump
    )]
    pub dir_arena: Box<Account<'info, DirArenaPda>>,
}

#[derive(Accounts)]
pub struct DeleteFile<'info> {
    pub authority: Signer<'info>,
    #[account(
        seeds = [b"walrusfs_root".as_ref(), authority.key().as_ref()],
        bump = walrusfs_root.bump
    )]
    pub walrusfs_root: Box<Account<'info, WalrusfsRootPda>>,
    #[account(
        mut,
        seeds = [b"root_children_files".as_ref(), walrusfs_root.key().as_ref()],
        bump = root_children_files.bump
    )]
    pub root_children_files: Box<Account<'info, ChildrenFilesPda>>,
    #[account( // For path traversal
        seeds = [b"root_children_directories".as_ref(), walrusfs_root.key().as_ref()],
        bump = root_children_directories.bump
    )]
    pub root_children_directories: Box<Account<'info, ChildrenDirectoriesPda>>,
    #[account(
        mut,
        seeds = [b"file_arena".as_ref(), walrusfs_root.key().as_ref()],
        bump = file_arena.bump
    )]
    pub file_arena: Box<Account<'info, FileArenaPda>>,
    #[account( // Dir arena is mutable as children_files within a DirObject might change
        mut,
        seeds = [b"dir_arena".as_ref(), walrusfs_root.key().as_ref()],
        bump = dir_arena.bump
    )]
    pub dir_arena: Box<Account<'info, DirArenaPda>>,
}

#[derive(Accounts)]
pub struct DeleteDir<'info> {
    pub authority: Signer<'info>,
    #[account(
        seeds = [b"walrusfs_root".as_ref(), authority.key().as_ref()],
        bump = walrusfs_root.bump
    )]
    pub walrusfs_root: Box<Account<'info, WalrusfsRootPda>>,
    #[account(
        mut, // For deleting dir at root or for path traversal if parent is root
        seeds = [b"root_children_directories".as_ref(), walrusfs_root.key().as_ref()],
        bump = root_children_directories.bump
    )]
    pub root_children_directories: Box<Account<'info, ChildrenDirectoriesPda>>,
    #[account(
        mut,
        seeds = [b"file_arena".as_ref(), walrusfs_root.key().as_ref()],
        bump = file_arena.bump
    )]
    pub file_arena: Box<Account<'info, FileArenaPda>>,
    #[account(
        mut,
        seeds = [b"dir_arena".as_ref(), walrusfs_root.key().as_ref()],
        bump = dir_arena.bump
    )]
    pub dir_arena: Box<Account<'info, DirArenaPda>>,
}

#[derive(Accounts)]
pub struct GetDirAll<'info> {
    // Inherits structure from ReadUserFileSystem
    /// CHECK: Owner of the filesystem.
    pub owner: AccountInfo<'info>,
    #[account(seeds = [b"walrusfs_root".as_ref(), owner.key().as_ref()], bump = walrusfs_root.bump)]
    pub walrusfs_root: Box<Account<'info, WalrusfsRootPda>>,
    #[account(seeds = [b"root_children_files".as_ref(), walrusfs_root.key().as_ref()], bump = root_children_files.bump)]
    pub root_children_files: Box<Account<'info, ChildrenFilesPda>>,
    #[account(seeds = [b"root_children_directories".as_ref(), walrusfs_root.key().as_ref()], bump = root_children_directories.bump)]
    pub root_children_directories: Box<Account<'info, ChildrenDirectoriesPda>>,
    #[account(seeds = [b"file_arena".as_ref(), walrusfs_root.key().as_ref()], bump = file_arena.bump)]
    pub file_arena: Box<Account<'info, FileArenaPda>>,
    #[account(seeds = [b"dir_arena".as_ref(), walrusfs_root.key().as_ref()], bump = dir_arena.bump)]
    pub dir_arena: Box<Account<'info, DirArenaPda>>,
}

// ... All other `#[derive(Accounts)]` structs from your original code (UpdateEpoch, AddFile, AddDir, ListDir, Stat, RenameFile, RenameDir, DeleteFile, DeleteDir, GetDirAll)
// should be included here. Their definitions are unchanged, but they will now operate on PDAs containing Vecs.

// --- Helper Structs for return types (Unchanged, not repeated for brevity) ---
// DirListObjectAnchor, FileObjectExAnchor, DirObjectExAnchor, RecursiveDirListAnchor

#[derive(AnchorSerialize, AnchorDeserialize, Clone, Debug)]
pub struct DirListObjectAnchor {
    pub name: String,
    pub create_ts: u64,
    pub is_dir: bool,
    pub tags: Vec<String>,
    pub size: u64,
    pub walrus_blob_id: String,
    pub walrus_epoch_till: u64,
}

#[derive(AnchorSerialize, AnchorDeserialize, Clone, Debug)]
pub struct FileObjectExAnchor {
    pub id: u64,
    pub obj: FileObjectAnchor,
}

#[derive(AnchorSerialize, AnchorDeserialize, Clone, Debug)]
pub struct DirObjectExAnchor {
    pub id: u64,
    pub create_ts: u64,
    pub tags: Vec<String>,
    pub children_file_names: Vec<String>,
    pub children_file_ids: Vec<u64>,
    pub children_directory_names: Vec<String>,
    pub children_directory_ids: Vec<u64>,
}

#[derive(AnchorSerialize, AnchorDeserialize, Clone, Debug)]
pub struct RecursiveDirListAnchor {
    pub dirobj: u64,
    pub files: Vec<FileObjectExAnchor>,
    pub dirs: Vec<DirObjectExAnchor>,
}
// --- Events (Unchanged, not repeated for brevity) ---
#[event]
pub struct FileAlreadyExistsEvent {
    path: String,
    create_ts: u64,
    tags: Vec<String>,
    size: u64,
    walrus_blob_id: String,
    walrus_epoch_till: u64,
}
#[event]
pub struct FileAddedEvent {
    path: String,
    create_ts: u64,
    tags: Vec<String>,
    size: u64,
    walrus_blob_id: String,
    walrus_epoch_till: u64,
}
#[event]
pub struct DirAlreadyExistsEvent {
    path: String,
    create_ts: u64,
    tags: Vec<String>,
}
#[event]
pub struct DirAddedEvent {
    path: String,
    create_ts: u64,
    tags: Vec<String>,
}
#[event]
pub struct DeleteEvent {
    path: String,
}
// --- Errors (Unchanged, not repeated for brevity) ---
#[error_code]
pub enum WalrusFsError {
    #[msg("Path error or invalid path format.")]
    PathError,
    #[msg("Arena mismatch or object not found in arena.")]
    ArenaMismatchError,
    #[msg("File already exists at the specified path.")]
    FileAlreadyExists,
    #[msg("Directory already exists at the specified path.")]
    DirectoryAlreadyExists,
    #[msg("Unauthorized operation.")]
    Unauthorized,
    #[msg("Path not found.")]
    PathNotFound,
    #[msg("String length exceeds maximum allowed.")]
    StringTooLong,
    #[msg("Too many tags specified.")]
    TooManyTags,
    #[msg("Rename 'from' and 'to' paths must be in the same parent directory.")]
    RenamePathMismatch,
    #[msg("Cannot perform this operation directly on the root ('/') path.")]
    InvalidPathOperationOnRoot,
    #[msg("Bump seed not found.")] // Not explicitly used in this code, but good to have
    BumpError,
}

