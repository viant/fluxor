Parameters:
- directory: string (required) — base directory for resolving relative file paths in the patch
- patch: string (required) — patch text to apply (unified-diff or simplified patch format)

Custom patch language is a stripped‑down, file‑oriented diff format designed to be easy to parse and safe to apply. You can think of it as a high‑level envelope:

*** Begin Patch
[ one or more file sections ]
*** End Patch

Within that envelope, you get a sequence of file operations.
You MUST include a header to specify the action you are taking.
Each operation starts with one of three headers:

*** Add File: <path> - create a new file. Every following line is a + line (the initial contents).
*** Delete File: <path> - remove an existing file. Nothing follows.
*** Update File: <path> - patch an existing file in place (optionally with a rename).

May be immediately followed by *** Move to: <new path> if you want to rename the file.
Then one or more “hunks”, each introduced by @@ (optionally followed by a hunk header).
Within a hunk each line starts with:

- for inserted text,

* for removed text, or
  space ( ) for context.
  At the end of a truncated hunk you can emit *** End of File.

Patch     := Begin { FileOp } End
Begin     := "*** Begin Patch" NEWLINE
End       := "*** End Patch" NEWLINE
FileOp    := AddFile | DeleteFile | UpdateFile
AddFile   := "*** Add File: " relPath NEWLINE { "+" line NEWLINE }
DeleteFile:= "*** Delete File: " relPath NEWLINE
UpdateFile:= "*** Update File: " relPath NEWLINE [ MoveTo ] { Hunk }
MoveTo    := "*** Move to: " relPath NEWLINE
Hunk      := "@@" [ header ] NEWLINE { HunkLine } [ "*** End of File" NEWLINE ]
HunkLine  := (" " | "-" | "+") text NEWLINE

relPath   := a relative file path (absolute paths are not allowed)

A full patch can combine several operations:

*** Begin Patch
*** Add File: hello.txt
+Hello world
*** Update File: src/app.py
*** Move to: src/main.py
@@ def greet():
-print("Hi")
+print("Hello, world!")
*** Delete File: obsolete.txt
*** End Patch

It is important to remember:

- You must include a header with your intended action (Add/Delete/Update)
- You must prefix new lines with `+` even when creating a new file
- All <relPath> values MUST be relative (never absolute)
- If a Directory param is provided, all paths are resolved relative to that directory