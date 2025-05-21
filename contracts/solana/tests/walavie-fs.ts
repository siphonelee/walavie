// tests/walrusfs-anchor.ts
import * as anchor from "@coral-xyz/anchor";
import { Program, BN, web3, ProgramError } from "@coral-xyz/anchor";
import { WalrusfsAnchor, IDL } from "../target/types/walrusfs_anchor"; // Adjust path if needed
import { expect } from "chai";

// --- Helper Functions ---
async function expectError(promise: Promise<any>, expectedErrorName: string) {
  try {
    await promise;
    expect.fail(`Expected an error but promise succeeded.`);
  } catch (err) {
    // Check if it's an Anchor ProgramError
    if (err instanceof ProgramError) {
      const anchorError = ProgramError.parse(err);
      if (anchorError && anchorError.msg.includes(expectedErrorName)) {
        // Error matches, test passes
        return;
      } else if (anchorError) {
        console.error("Unexpected Anchor Error:", anchorError);
        expect.fail(`Expected error name '${expectedErrorName}' but got '${anchorError.msg}' (code: ${anchorError.code})`);
      }
    }
    // Fallback for other error types or if ProgramError.parse fails
    if (err.message && err.message.includes(expectedErrorName)) {
      // Error matches, test passes
      return;
    }
    console.error("Unexpected Error:", err);
    expect.fail(`Expected error name '${expectedErrorName}' but got a different error: ${err.message || err}`);
  }
}


describe("walrusfs-anchor", () => {
  // Configure the client to use the local cluster.
  const provider = anchor.AnchorProvider.env();
  anchor.setProvider(provider);

  const program = anchor.workspace.WalrusfsAnchor as Program<WalrusfsAnchor>;
  const payer = provider.wallet as anchor.Wallet; // Payer and initial authority

  // PDA addresses will be derived in tests
  let walrusfsRootPda: web3.PublicKey;
  let rootChildrenFilesPda: web3.PublicKey;
  let rootChildrenDirectoriesPda: web3.PublicKey;
  let fileArenaPda: web3.PublicKey;
  let dirArenaPda: web3.PublicKey;

  const MAX_TAGS = 5;
  const MAX_STRING_LEN = 64;


  before(async () => {
    // Derive PDA addresses based on the payer's public key
    [walrusfsRootPda] = await web3.PublicKey.findProgramAddressSync(
      [Buffer.from("walrusfs_root"), payer.publicKey.toBuffer()],
      program.programId
    );
    [rootChildrenFilesPda] = await web3.PublicKey.findProgramAddressSync(
      [Buffer.from("root_children_files"), walrusfsRootPda.toBuffer()],
      program.programId
    );
    [rootChildrenDirectoriesPda] = await web3.PublicKey.findProgramAddressSync(
      [Buffer.from("root_children_directories"), walrusfsRootPda.toBuffer()],
      program.programId
    );
    [fileArenaPda] = await web3.PublicKey.findProgramAddressSync(
      [Buffer.from("file_arena"), walrusfsRootPda.toBuffer()],
      program.programId
    );
    [dirArenaPda] = await web3.PublicKey.findProgramAddressSync(
      [Buffer.from("dir_arena"), walrusfsRootPda.toBuffer()],
      program.programId
    );
  });

  it("Is initialized!", async () => {
    try {
        await program.methods
        .initializeWalrusfs()
        .accounts({
          walrusfsRoot: walrusfsRootPda,
          rootChildrenFiles: rootChildrenFilesPda,
          rootChildrenDirectories: rootChildrenDirectoriesPda,
          fileArena: fileArenaPda,
          dirArena: dirArenaPda,
          payer: payer.publicKey,
          systemProgram: web3.SystemProgram.programId,
        })
        .signers([payer.payer]) // if payer is a Keypair an not Wallet
        .rpc();
    } catch (e) {
        // Allow re-initialization for easier testing, but log if it's not the first time
        if (!e.toString().includes("custom program error: 0x0")) { // 0x0 is AccountAlreadyInitialized
            console.error("Initialization failed for a reason other than already initialized:", e);
            throw e;
        }
        console.log("WalrusFS already initialized for this payer, continuing tests.");
    }

    const rootAccount = await program.account.walrusfsRootPda.fetch(walrusfsRootPda);
    expect(rootAccount.currentEpoch.toNumber()).to.equal(0);
    expect(rootAccount.objIdCounter.toNumber()).to.equal(0);
    expect(rootAccount.authority.equals(payer.publicKey)).to.be.true;

    const rootFiles = await program.account.childrenFilesPda.fetch(rootChildrenFilesPda);
    expect(rootFiles.data).to.be.an('array').that.is.empty;

    const rootDirs = await program.account.childrenDirectoriesPda.fetch(rootChildrenDirectoriesPda);
    expect(rootDirs.data).to.be.an('array').that.is.empty;

    const fileArena = await program.account.fileArenaPda.fetch(fileArenaPda);
    expect(fileArena.data).to.be.an('array').that.is.empty;

    const dirArena = await program.account.dirArenaPda.fetch(dirArenaPda);
    expect(dirArena.data).to.be.an('array').that.is.empty;
  });

  it("Updates epoch", async () => {
    const newEpoch = new BN(123);
    await program.methods
      .updateEpoch(newEpoch)
      .accounts({
        walrusfsRoot: walrusfsRootPda,
        authority: payer.publicKey,
      })
      .rpc();

    const rootAccount = await program.account.walrusfsRootPda.fetch(walrusfsRootPda);
    expect(rootAccount.currentEpoch.eq(newEpoch)).to.be.true;
  });

  it("Fails to update epoch with wrong authority", async () => {
    const wrongAuthority = web3.Keypair.generate();
    const newEpoch = new BN(456);
    // Airdrop to wrongAuthority if needed for it to be a signer
    // await provider.connection.requestAirdrop(wrongAuthority.publicKey, web3.LAMPORTS_PER_SOL);
    // await new Promise(resolve => setTimeout(resolve, 1000)); // Await airdrop confirmation

    await expectError(
        program.methods
        .updateEpoch(newEpoch)
        .accounts({
            walrusfsRoot: walrusfsRootPda, // This root belongs to 'payer'
            authority: wrongAuthority.publicKey,
        })
        .signers([wrongAuthority]) // Sign with the wrong authority
        .rpc(),
        "ConstraintSeeds" // Anchor will fail on seed constraint before custom Unauthorized error
    );
  });


  // --- File Operations ---
  it("Adds a file to the root directory", async () => {
    const path = "/file1.txt";
    const tags = ["doc", "important"];
    const size = new BN(1024);
    const walrusBlobId = "blob_id_1";
    const endEpoch = new BN(200);
    const overwrite = false;

    await program.methods
      .addFile(path, tags, size, walrusBlobId, endEpoch, overwrite)
      .accounts({
        walrusfsRoot: walrusfsRootPda,
        rootChildrenFiles: rootChildrenFilesPda,
        rootChildrenDirectories: rootChildrenDirectoriesPda, // Read-only for path resolution
        fileArena: fileArenaPda,
        dirArena: dirArenaPda, // Potentially mutable if parent is not root
        authority: payer.publicKey,
      })
      .rpc();

    const rootAccount = await program.account.walrusfsRootPda.fetch(walrusfsRootPda);
    expect(rootAccount.objIdCounter.toNumber()).to.equal(1); // First object

    const rootFiles = await program.account.childrenFilesPda.fetch(rootChildrenFilesPda);
    expect(rootFiles.data.length).to.equal(1);
    expect(rootFiles.data[0].key).to.equal("file1.txt");
    expect(rootFiles.data[0].value.toNumber()).to.equal(1);

    const fileArena = await program.account.fileArenaPda.fetch(fileArenaPda);
    expect(fileArena.data.length).to.equal(1);
    expect(fileArena.data[0].key.toNumber()).to.equal(1);
    expect(fileArena.data[0].value.tags).to.deep.equal(tags);
    expect(fileArena.data[0].value.size.eq(size)).to.be.true;
    expect(fileArena.data[0].value.walrusBlobId).to.equal(walrusBlobId);
  });

  it("Fails to add an existing file without overwrite flag", async () => {
    const path = "/file1.txt"; // Same as before
    // ... other params
    await expectError(
      program.methods
        .addFile(path, [], new BN(0), "", new BN(0), false)
        .accounts({
          walrusfsRoot: walrusfsRootPda,
          rootChildrenFiles: rootChildrenFilesPda,
          rootChildrenDirectories: rootChildrenDirectoriesPda,
          fileArena: fileArenaPda,
          dirArena: dirArenaPda,
          authority: payer.publicKey,
        })
        .rpc(),
      "FileAlreadyExists"
    );
  });

  it("Overwrites an existing file with overwrite flag", async () => {
    const path = "/file1.txt";
    const newTags = ["updated"];
    const newSize = new BN(2048);
    const newWalrusBlobId = "blob_id_1_updated";
    const newEndEpoch = new BN(250);

    await program.methods
      .addFile(path, newTags, newSize, newWalrusBlobId, newEndEpoch, true)
      .accounts({
        walrusfsRoot: walrusfsRootPda,
        rootChildrenFiles: rootChildrenFilesPda,
        rootChildrenDirectories: rootChildrenDirectoriesPda,
        fileArena: fileArenaPda,
        dirArena: dirArenaPda,
        authority: payer.publicKey,
      })
      .rpc();

    const rootAccount = await program.account.walrusfsRootPda.fetch(walrusfsRootPda);
    // objIdCounter should increment because a new object ID is used for the overwritten file
    expect(rootAccount.objIdCounter.toNumber()).to.equal(2);

    const rootFiles = await program.account.childrenFilesPda.fetch(rootChildrenFilesPda);
    expect(rootFiles.data.length).to.equal(1); // Still one file in root listing
    expect(rootFiles.data[0].key).to.equal("file1.txt");
    expect(rootFiles.data[0].value.toNumber()).to.equal(2); // Points to the new object ID

    const fileArena = await program.account.fileArenaPda.fetch(fileArenaPda);
    // Old file object (ID 1) should be removed, new one (ID 2) added.
    expect(fileArena.data.length).to.equal(1); // Only the new version remains
    const newFileInArena = fileArena.data.find(f => f.key.toNumber() === 2);
    expect(newFileInArena).to.exist;
    expect(newFileInArena.value.tags).to.deep.equal(newTags);
    expect(newFileInArena.value.size.eq(newSize)).to.be.true;
  });


  // --- Directory Operations ---
  it("Adds a directory to the root", async () => {
    const path = "/dir1";
    const tags = ["folder"];

    await program.methods
      .addDir(path, tags)
      .accounts({
        walrusfsRoot: walrusfsRootPda,
        rootChildrenDirectories: rootChildrenDirectoriesPda,
        dirArena: dirArenaPda,
        authority: payer.publicKey,
      })
      .rpc();

    const rootAccount = await program.account.walrusfsRootPda.fetch(walrusfsRootPda);
    expect(rootAccount.objIdCounter.toNumber()).to.equal(3); // objIdCounter increments

    const rootDirs = await program.account.childrenDirectoriesPda.fetch(rootChildrenDirectoriesPda);
    expect(rootDirs.data.length).to.equal(1);
    expect(rootDirs.data[0].key).to.equal("dir1");
    expect(rootDirs.data[0].value.toNumber()).to.equal(3);

    const dirArena = await program.account.dirArenaPda.fetch(dirArenaPda);
    expect(dirArena.data.length).to.equal(1);
    expect(dirArena.data[0].key.toNumber()).to.equal(3);
    expect(dirArena.data[0].value.tags).to.deep.equal(tags);
    expect(dirArena.data[0].value.childrenFiles).to.be.an('array').that.is.empty;
    expect(dirArena.data[0].value.childrenDirectories).to.be.an('array').that.is.empty;
  });

  it("Fails to add an existing directory", async () => {
    const path = "/dir1"; // Same as before
    await expectError(
      program.methods
        .addDir(path, [])
        .accounts({
            walrusfsRoot: walrusfsRootPda,
            rootChildrenDirectories: rootChildrenDirectoriesPda,
            dirArena: dirArenaPda,
            authority: payer.publicKey,
        })
        .rpc(),
      "DirectoryAlreadyExists"
    );
  });

  it("Adds a file to a subdirectory", async () => {
    const path = "/dir1/subfile.txt";
    const tags = ["sub"];
    const size = new BN(512);
    const walrusBlobId = "blob_sub_1";
    const endEpoch = new BN(300);

    await program.methods
      .addFile(path, tags, size, walrusBlobId, endEpoch, false)
      .accounts({
        walrusfsRoot: walrusfsRootPda,
        rootChildrenFiles: rootChildrenFilesPda, // Not directly used for subdirs
        rootChildrenDirectories: rootChildrenDirectoriesPda,
        fileArena: fileArenaPda,
        dirArena: dirArenaPda,
        authority: payer.publicKey,
      })
      .rpc();

    const rootAccount = await program.account.walrusfsRootPda.fetch(walrusfsRootPda);
    expect(rootAccount.objIdCounter.toNumber()).to.equal(4); // Incremented

    const dirArena = await program.account.dirArenaPda.fetch(dirArenaPda);
    const parentDirObject = dirArena.data.find(d => d.key.toNumber() === 3); // dir1's ID
    expect(parentDirObject).to.exist;
    expect(parentDirObject.value.childrenFiles.length).to.equal(1);
    expect(parentDirObject.value.childrenFiles[0].key).to.equal("subfile.txt");
    expect(parentDirObject.value.childrenFiles[0].value.toNumber()).to.equal(4);

    const fileArena = await program.account.fileArenaPda.fetch(fileArenaPda);
    const subFileInArena = fileArena.data.find(f => f.key.toNumber() === 4);
    expect(subFileInArena).to.exist;
    expect(subFileInArena.value.tags).to.deep.equal(tags);
  });

  it("Adds a subdirectory", async () => {
    const path = "/dir1/subdir1";
    const tags = ["nested"];

    await program.methods
      .addDir(path, tags)
      .accounts({
        walrusfsRoot: walrusfsRootPda,
        rootChildrenDirectories: rootChildrenDirectoriesPda,
        dirArena: dirArenaPda,
        authority: payer.publicKey,
      })
      .rpc();
    
    const rootAccount = await program.account.walrusfsRootPda.fetch(walrusfsRootPda);
    expect(rootAccount.objIdCounter.toNumber()).to.equal(5); // Incremented

    const dirArena = await program.account.dirArenaPda.fetch(dirArenaPda);
    const parentDirObject = dirArena.data.find(d => d.key.toNumber() === 3); // dir1's ID
    expect(parentDirObject).to.exist;
    expect(parentDirObject.value.childrenDirectories.length).to.equal(1);
    expect(parentDirObject.value.childrenDirectories[0].key).to.equal("subdir1");
    expect(parentDirObject.value.childrenDirectories[0].value.toNumber()).to.equal(5);

    const subDirObject = dirArena.data.find(d => d.key.toNumber() === 5); // subdir1's ID
    expect(subDirObject).to.exist;
    expect(subDirObject.value.tags).to.deep.equal(tags);
  });

  // --- Listing and Stat Operations ---
  it("Lists the root directory", async () => {
    const results = await program.methods
      .listDir("/")
      .accounts({
        owner: payer.publicKey, // For read operations, owner is used to derive PDAs
        walrusfsRoot: walrusfsRootPda,
        rootChildrenFiles: rootChildrenFilesPda,
        rootChildrenDirectories: rootChildrenDirectoriesPda,
        fileArena: fileArenaPda,
        dirArena: dirArenaPda,
      })
      .view(); // Using .view() for read-only operations if supported and no signers needed

    expect(results.length).to.equal(2); // file1.txt, dir1
    const file1 = results.find(r => r.name === "file1.txt");
    const dir1 = results.find(r => r.name === "dir1");

    expect(file1).to.exist;
    expect(file1.isDir).to.be.false;
    expect(file1.size.toNumber()).to.equal(2048); // overwritten size

    expect(dir1).to.exist;
    expect(dir1.isDir).to.be.true;
  });

  it("Lists a subdirectory '/dir1/'", async () => {
    const results = await program.methods
      .listDir("/dir1/") // or "/dir1"
      .accounts({
        owner: payer.publicKey,
        walrusfsRoot: walrusfsRootPda,
        rootChildrenFiles: rootChildrenFilesPda,
        rootChildrenDirectories: rootChildrenDirectoriesPda,
        fileArena: fileArenaPda,
        dirArena: dirArenaPda,
      })
      .view();

    expect(results.length).to.equal(2); // subfile.txt, subdir1
    const subfile = results.find(r => r.name === "subfile.txt");
    const subdir1 = results.find(r => r.name === "subdir1");

    expect(subfile).to.exist;
    expect(subfile.isDir).to.be.false;
    expect(subdir1).to.exist;
    expect(subdir1.isDir).to.be.true;
  });

  it("Gets stat for a file", async () => {
    const statResult = await program.methods
      .stat("/file1.txt")
      .accounts({
        owner: payer.publicKey,
        walrusfsRoot: walrusfsRootPda,
        rootChildrenFiles: rootChildrenFilesPda,
        rootChildrenDirectories: rootChildrenDirectoriesPda,
        fileArena: fileArenaPda,
        dirArena: dirArenaPda,
      })
      .view();

    expect(statResult.name).to.equal("file1.txt");
    expect(statResult.isDir).to.be.false;
    expect(statResult.size.toNumber()).to.equal(2048);
    expect(statResult.tags).to.deep.equal(["updated"]);
  });

  it("Gets stat for a directory", async () => {
    const statResult = await program.methods
      .stat("/dir1") // or "/dir1/"
      .accounts({
        owner: payer.publicKey,
        walrusfsRoot: walrusfsRootPda,
        rootChildrenFiles: rootChildrenFilesPda,
        rootChildrenDirectories: rootChildrenDirectoriesPda,
        fileArena: fileArenaPda,
        dirArena: dirArenaPda,
      })
      .view();

    expect(statResult.name).to.equal("dir1");
    expect(statResult.isDir).to.be.true;
    expect(statResult.tags).to.deep.equal(["folder"]);
  });

  it("Fails to get stat for a non-existent path", async () => {
    await expectError(
      program.methods
      .stat("/nonexistent.txt")
      .accounts({
        owner: payer.publicKey,
        walrusfsRoot: walrusfsRootPda,
        rootChildrenFiles: rootChildrenFilesPda,
        rootChildrenDirectories: rootChildrenDirectoriesPda,
        fileArena: fileArenaPda,
        dirArena: dirArenaPda,
      })
      .rpc(), 
      "PathNotFound"
    );
  });

  // --- Rename Operations ---
  it("Renames a file in the root", async () => {
    const fromPath = "/file1.txt";
    const toPath = "/renamed_file1.txt";

    await program.methods
      .renameFile(fromPath, toPath)
      .accounts({
        walrusfsRoot: walrusfsRootPda,
        rootChildrenFiles: rootChildrenFilesPda,
        rootChildrenDirectories: rootChildrenDirectoriesPda,
        dirArena: dirArenaPda,
        authority: payer.publicKey,
      })
      .rpc();

    const rootFiles = await program.account.childrenFilesPda.fetch(rootChildrenFilesPda);
    const renamedFile = rootFiles.data.find(f => f.key === "renamed_file1.txt");
    const oldFile = rootFiles.data.find(f => f.key === "file1.txt");
    expect(renamedFile).to.exist;
    expect(renamedFile.value.toNumber()).to.equal(2); // ID should be preserved
    expect(oldFile).to.not.exist;

    // Check stat of new name
    const statResult = await program.methods.stat(toPath).accounts({ owner: payer.publicKey, walrusfsRoot: walrusfsRootPda, rootChildrenFiles: rootChildrenFilesPda, rootChildrenDirectories: rootChildrenDirectoriesPda, fileArena: fileArenaPda, dirArena: dirArenaPda }).view();
    expect(statResult.name).to.equal("renamed_file1.txt");
  });

  it("Renames a directory in a subdirectory", async () => {
    const fromPath = "/dir1/subdir1";
    const toPath = "/dir1/renamed_subdir1";

    await program.methods
      .renameDir(fromPath, toPath)
      .accounts({
        walrusfsRoot: walrusfsRootPda,
        rootChildrenDirectories: rootChildrenDirectoriesPda,
        dirArena: dirArenaPda,
        authority: payer.publicKey,
      })
      .rpc();

    const dirArena = await program.account.dirArenaPda.fetch(dirArenaPda);
    const parentDir = dirArena.data.find(d => d.key.toNumber() === 3); // dir1's ID
    expect(parentDir).to.exist;
    const renamedDirEntry = parentDir.value.childrenDirectories.find(cd => cd.key === "renamed_subdir1");
    const oldDirEntry = parentDir.value.childrenDirectories.find(cd => cd.key === "subdir1");
    expect(renamedDirEntry).to.exist;
    expect(renamedDirEntry.value.toNumber()).to.equal(5); // ID preserved
    expect(oldDirEntry).to.not.exist;
  });

  it("Fails to rename file if 'to_path' already exists", async () => {
    // Add a temporary file that will cause conflict
    await program.methods.addFile("/temp_file.txt", [], new BN(10), "temp_blob", new BN(400), false)
        .accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenFiles: rootChildrenFilesPda, rootChildrenDirectories: rootChildrenDirectoriesPda, fileArena: fileArenaPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc();

    await expectError(
        program.methods
        .renameFile("/renamed_file1.txt", "/temp_file.txt")
        .accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenFiles: rootChildrenFilesPda, rootChildrenDirectories: rootChildrenDirectoriesPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc(),
        "FileAlreadyExists"
    );
     // cleanup
    await program.methods.deleteFile("/temp_file.txt")
        .accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenFiles: rootChildrenFilesPda, rootChildrenDirectories: rootChildrenDirectoriesPda, fileArena: fileArenaPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc();
  });

  // --- Delete Operations ---
  it("Deletes a file", async () => {
    const path = "/renamed_file1.txt";
    // file ID was 2
    await program.methods
      .deleteFile(path)
      .accounts({
        walrusfsRoot: walrusfsRootPda,
        rootChildrenFiles: rootChildrenFilesPda,
        rootChildrenDirectories: rootChildrenDirectoriesPda,
        fileArena: fileArenaPda,
        dirArena: dirArenaPda,
        authority: payer.publicKey,
      })
      .rpc();

    const rootFiles = await program.account.childrenFilesPda.fetch(rootChildrenFilesPda);
    expect(rootFiles.data.find(f => f.key === "renamed_file1.txt")).to.not.exist;

    const fileArena = await program.account.fileArenaPda.fetch(fileArenaPda);
    // File with ID 2 should be gone. subfile.txt (ID 4) should remain.
    expect(fileArena.data.find(f => f.key.toNumber() === 2)).to.not.exist;
    expect(fileArena.data.length).to.equal(1); // Only subfile.txt remains
  });

  it("Deletes an empty directory", async () => {
    // First add an empty dir
    const emptyDirPath = "/dir_empty";
    await program.methods.addDir(emptyDirPath, []).accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenDirectories: rootChildrenDirectoriesPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc();
    const rootAccountBefore = await program.account.walrusfsRootPda.fetch(walrusfsRootPda);
    const emptyDirId = rootAccountBefore.objIdCounter; // ID of /dir_empty

    await program.methods
      .deleteDir(emptyDirPath)
      .accounts({
        walrusfsRoot: walrusfsRootPda,
        rootChildrenDirectories: rootChildrenDirectoriesPda,
        fileArena: fileArenaPda,
        dirArena: dirArenaPda,
        authority: payer.publicKey,
      })
      .rpc();

    const rootDirs = await program.account.childrenDirectoriesPda.fetch(rootChildrenDirectoriesPda);
    expect(rootDirs.data.find(d => d.key === "dir_empty")).to.not.exist;

    const dirArena = await program.account.dirArenaPda.fetch(dirArenaPda);
    expect(dirArena.data.find(d => d.key.eq(emptyDirId))).to.not.exist;
  });

  it("Deletes a non-empty directory recursively", async () => {
    // /dir1 contains /dir1/subfile.txt (ID 4) and /dir1/renamed_subdir1 (ID 5)
    // renamed_subdir1 is empty
    const pathToDelete = "/dir1";

    // Sanity check before delete
    let dirArenaState = await program.account.dirArenaPda.fetch(dirArenaPda);
    expect(dirArenaState.data.find(d => d.key.toNumber() === 3 /* /dir1 */)).to.exist;
    expect(dirArenaState.data.find(d => d.key.toNumber() === 5 /* /dir1/renamed_subdir1 */)).to.exist;
    let fileArenaState = await program.account.fileArenaPda.fetch(fileArenaPda);
    expect(fileArenaState.data.find(f => f.key.toNumber() === 4 /* /dir1/subfile.txt */)).to.exist;


    await program.methods
      .deleteDir(pathToDelete)
      .accounts({
        walrusfsRoot: walrusfsRootPda,
        rootChildrenDirectories: rootChildrenDirectoriesPda,
        fileArena: fileArenaPda,
        dirArena: dirArenaPda,
        authority: payer.publicKey,
      })
      .rpc();

    const rootDirs = await program.account.childrenDirectoriesPda.fetch(rootChildrenDirectoriesPda);
    expect(rootDirs.data.find(d => d.key === "dir1")).to.not.exist; // /dir1 removed from root listing
    expect(rootDirs.data.length).to.equal(0); // Root should now be empty of dirs

    dirArenaState = await program.account.dirArenaPda.fetch(dirArenaPda);
    expect(dirArenaState.data.find(d => d.key.toNumber() === 3 /* /dir1 */)).to.not.exist;
    expect(dirArenaState.data.find(d => d.key.toNumber() === 5 /* /dir1/renamed_subdir1 */)).to.not.exist;
    expect(dirArenaState.data.length).to.equal(0); // All dirs should be gone from arena

    fileArenaState = await program.account.fileArenaPda.fetch(fileArenaPda);
    expect(fileArenaState.data.find(f => f.key.toNumber() === 4 /* /dir1/subfile.txt */)).to.not.exist;
    expect(fileArenaState.data.length).to.equal(0); // All files should be gone from arena
  });

  it("Fails to delete non-existent file/dir", async () => {
    await expectError(
        program.methods.deleteFile("/non_existent_file.txt")
        .accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenFiles: rootChildrenFilesPda, rootChildrenDirectories: rootChildrenDirectoriesPda, fileArena: fileArenaPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc(),
        "PathNotFound"
    );
    await expectError(
        program.methods.deleteDir("/non_existent_dir")
        .accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenDirectories: rootChildrenDirectoriesPda, fileArena: fileArenaPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc(),
        "PathNotFound"
    );
  });

  // --- GetDirAll ---
  it("Gets all directory contents recursively (after re-populating)", async () => {
    // Re-populate for this test
    await program.methods.addDir("/level1", ["l1_tag"]).accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenDirectories: rootChildrenDirectoriesPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc(); // ID 6
    await program.methods.addFile("/level1/fileA.txt", ["file_a"], new BN(100), "blobA", new BN(500), false).accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenFiles: rootChildrenFilesPda, rootChildrenDirectories: rootChildrenDirectoriesPda, fileArena: fileArenaPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc(); // ID 7
    await program.methods.addDir("/level1/level2", ["l2_tag"]).accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenDirectories: rootChildrenDirectoriesPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc(); // ID 8
    await program.methods.addFile("/level1/level2/fileB.txt", ["file_b"], new BN(200), "blobB", new BN(600), false).accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenFiles: rootChildrenFilesPda, rootChildrenDirectories: rootChildrenDirectoriesPda, fileArena: fileArenaPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc(); // ID 9

    const result = await program.methods
      .getDirAll("/level1")
      .accounts({
        owner: payer.publicKey,
        walrusfsRoot: walrusfsRootPda,
        rootChildrenFiles: rootChildrenFilesPda,
        rootChildrenDirectories: rootChildrenDirectoriesPda,
        fileArena: fileArenaPda,
        dirArena: dirArenaPda,
      })
      .view();
    
    expect(result.dirobj.toNumber()).to.equal(8); // ID of /level1

    expect(result.files.length).to.equal(2);
    const fileA = result.files.find(f => f.id.toNumber() === 9);
    const fileB = result.files.find(f => f.id.toNumber() === 11);
    expect(fileA).to.exist;
    expect(fileA.obj.walrusBlobId).to.equal("blobA");
    expect(fileB).to.exist;
    expect(fileB.obj.walrusBlobId).to.equal("blobB");

    expect(result.dirs.length).to.equal(2); // /level1 and /level1/level2
    const dirLevel1 = result.dirs.find(d => d.id.toNumber() === 8);
    const dirLevel2 = result.dirs.find(d => d.id.toNumber() === 10);
    expect(dirLevel1).to.exist;
    expect(dirLevel1.childrenFileNames).to.include("fileA.txt");
    expect(dirLevel1.childrenDirectoryNames).to.include("level2");

    expect(dirLevel2).to.exist;
    expect(dirLevel2.childrenFileNames).to.include("fileB.txt");
    expect(dirLevel2.childrenDirectoryNames).to.be.empty;
  });

  // --- Path Validation and Edge Cases ---
  it("Fails operations with invalid paths", async () => {
    const invalidPaths = ["", "no_slash", "/path//double_slash", `/${"a".repeat(MAX_STRING_LEN * 6)}`];
    for (const p of invalidPaths) {
        await expectError(program.methods.addFile(p, [], new BN(0), "", new BN(0), false)
            .accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenFiles: rootChildrenFilesPda, rootChildrenDirectories: rootChildrenDirectoriesPda, fileArena: fileArenaPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc(), "PathError");
        await expectError(program.methods.addDir(p, [])
            .accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenDirectories: rootChildrenDirectoriesPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc(), "PathError");
    }
    // Operation on root
    await expectError(program.methods.addFile("/", [], new BN(0), "", new BN(0), false)
        .accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenFiles: rootChildrenFilesPda, rootChildrenDirectories: rootChildrenDirectoriesPda, fileArena: fileArenaPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc(), "InvalidPathOperationOnRoot");
  });

  it("Fails operations with too many tags or too long strings", async () => {
    const tooManyTags = Array(MAX_TAGS + 1).fill("tag");
    const longString = "a".repeat(MAX_STRING_LEN + 1);

    await expectError(program.methods.addFile("/tags_test.txt", tooManyTags, new BN(0), "blob", new BN(0), false)
        .accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenFiles: rootChildrenFilesPda, rootChildrenDirectories: rootChildrenDirectoriesPda, fileArena: fileArenaPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc(), "TooManyTags");

    await expectError(program.methods.addFile("/long_blob.txt", [], new BN(0), longString, new BN(0), false)
        .accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenFiles: rootChildrenFilesPda, rootChildrenDirectories: rootChildrenDirectoriesPda, fileArena: fileArenaPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc(), "StringTooLong");

    await expectError(program.methods.addFile("/long_tag.txt", [longString], new BN(0), "blob", new BN(0), false)
        .accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenFiles: rootChildrenFilesPda, rootChildrenDirectories: rootChildrenDirectoriesPda, fileArena: fileArenaPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc(), "StringTooLong");
  });

  // Add more tests for edge cases for rename, delete involving paths like "/" or non-existent parents
  it("Fails rename if 'from_path' does not exist", async () => {
    await expectError(
        program.methods.renameFile("/non_existent_from.txt", "/some_to.txt")
        .accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenFiles: rootChildrenFilesPda, rootChildrenDirectories: rootChildrenDirectoriesPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc(),
        "PathNotFound" // Or could be ConstraintSeeds if path resolution fails earlier for PDA derivation
    );
  });

  it("Fails rename if paths are in different directories", async () => {
    // Ensure /level4 exists for this test
    if (!((await program.account.dirArenaPda.fetch(dirArenaPda)).data.find(d => d.value.childrenDirectories.find(k => k.key == "level4") ))) {
      await program.methods.addDir("/level4", []).accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenDirectories: rootChildrenDirectoriesPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc();
    }
     // Ensure /level4/fileA.txt exists from previous test or add it
    if (!(await program.account.dirArenaPda.fetch(dirArenaPda)).data.find(d => d.value.childrenFiles.find(k => k.key == "fileC.txt")  )) {
       await program.methods.addFile("/level4/fileC.txt", [], new BN(100), "blobA", new BN(500), false).accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenFiles: rootChildrenFilesPda, rootChildrenDirectories: rootChildrenDirectoriesPda, fileArena: fileArenaPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc();
    }
    // Add a root file to attempt renaming into subdir
    await program.methods.addFile("/root_file_for_rename.txt", [], new BN(10), "root_blob", new BN(1000), false)
        .accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenFiles: rootChildrenFilesPda, rootChildrenDirectories: rootChildrenDirectoriesPda, fileArena: fileArenaPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc();
   
    await expectError(
        program.methods.renameFile("/root_file_for_rename.txt", "/level1/new_name.txt")
        .accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenFiles: rootChildrenFilesPda, rootChildrenDirectories: rootChildrenDirectoriesPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc(),
        "RenamePathMismatch"
    );
    // cleanup
    await program.methods.deleteFile("/root_file_for_rename.txt")
      .accounts({ walrusfsRoot: walrusfsRootPda, rootChildrenFiles: rootChildrenFilesPda, rootChildrenDirectories: rootChildrenDirectoriesPda, fileArena: fileArenaPda, dirArena: dirArenaPda, authority: payer.publicKey }).rpc();

  });
});