# Part A2: Git Architecture & Command Flow Analysis

## 1. Overview: The Git Four-Box Model

Modern software and data engineering pipelines move code across distinct logical stages before production execution. This architecture follows the four-box model:

```
[ 1. Working Folder ]  ──(git add)──>  [ 2. Staging Area / Local Git ]  ──(git commit)──>  [ 3. Local Repository ]
                                                                                                  │
                                                                                              (git push)
                                                                                                  ▼
[ 5. CI / CD Runner ]  <──(triggers)──  [ 4. Remote Git (GitHub) ]  <──(git pull)────────────────┘
```

Each Git command operates on specific boundaries between these areas.

---

## 2. Command Analysis Table

| Command | Where the change happens | What exactly happens? |
| :--- | :--- | :--- |
| `git init` | **Working Directory** (creates `.git/` folder) | Initializes a new, empty Git version control repository in the current root directory by generating the hidden `.git/` metadata directory containing the object database (`objects/`), reference heads (`refs/heads/`), template files, and default configuration (`config`). It does not modify, track, or commit any existing files in the working directory until `git add` is executed. |
| `git add train.py` | **Staging Area (Index)** | Reads the working tree version of `train.py`, computes its SHA-1/SHA-256 hash, creates a compressed blob object inside `.git/objects/`, and records the file path, permissions, and blob hash into the index (`.git/index`). The working directory file remains untouched; the change is now staged as part of the preparation for the next atomic commit snapshot. |
| `git commit -m "msg"` | **Local Repository** (`.git/objects/`, `refs/heads/`) | Bundles all current entries in the staging area (index) into a permanent tree object, creates an immutable commit object referencing that tree, author/committer metadata, timestamp, parent commit ID, and log message (`"msg"`). It then updates the current local branch pointer (e.g., `refs/heads/main`) to this new commit. The staging area and working directory remain clean, and zero network traffic occurs. |
| `git remote add origin URL` | **Local Configuration File** (`.git/config`) | Writes a new remote configuration section into `.git/config` mapping the nickname `origin` to the specified remote repository network `URL` along with default fetch refspecs (`+refs/heads/*:refs/remotes/origin/*`). It does not contact the remote server, transmit repository data, or verify remote accessibility at the moment of execution. |
| `git push -u origin main` | **Remote Repository** (GitHub) and **Local Tracking References** (`refs/remotes/origin/main`) | Negotiates missing commit objects with the remote server, securely transfers all commit, tree, and blob objects from the local branch to the remote repository, advances the remote `main` branch reference to match the local tip, and links the local `main` branch to track `origin/main` upstream (`-u`). |
| `git pull origin main` | **Remote Tracking Ref**, **Local Repository**, and **Working Directory** | Performs a two-step operation: first calls `git fetch origin main` to download any new commits and objects from GitHub to the remote-tracking branch `origin/main`, then immediately executes `git merge origin/main` (or rebase, if configured) into the currently checked-out local branch, updating the local commit history, staging area, and working tree files simultaneously. |

---

## 3. Conceptual Question & Answer

### Question:
> *After running `git commit`, can your teammate see your code on GitHub? Why or why not?*

### Answer:
**No, your teammate cannot see your code on GitHub after running `git commit`.**

#### Explanation:
Git is a distributed version control system (DVCS). When you execute `git commit`:
1. The snapshot and its associated metadata are strictly written to your **Local Repository** (inside your machine's private `.git/objects/` database).
2. The current local branch pointer (such as `main`) moves forward on your local workstation only.
3. No network packets are transmitted, and no synchronization with the **Remote Repository (GitHub)** takes place.

Until you explicitly run `git push origin main` (or the corresponding tracking push command), the remote repository on GitHub remains completely unaware of your new commit. Because your teammates inspect and pull changes from the shared remote repository on GitHub (or via automated CI/CD runners triggered by GitHub webhooks), they cannot observe, review, or integrate your code until the push operation successfully completes.
