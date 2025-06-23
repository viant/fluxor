package patch

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSession_BasicOperations(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "patch-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a test file
	testFile := filepath.Join(tempDir, "test.txt")
	originalContent := []byte("original content\n")
	err = os.WriteFile(testFile, originalContent, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create a new session
	session, err := NewSession()
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	// Test Update operation
	newContent := []byte("updated content\n")
	err = session.Update(testFile, newContent)
	if err != nil {
		t.Fatalf("Failed to update file: %v", err)
	}

	// Verify the file was updated
	content, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	assert.Equal(t, newContent, content, "File content should be updated")

	// Test Rollback operation
	err = session.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	// Verify the file was restored to original content
	content, err = os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	assert.Equal(t, originalContent, content, "File content should be restored to original")
}

func TestSession_Add(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "patch-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a new session
	session, err := NewSession()
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	// Test Add operation
	newFile := filepath.Join(tempDir, "new.txt")
	content := []byte("new file content\n")
	err = session.Add(newFile, content)
	if err != nil {
		t.Fatalf("Failed to add file: %v", err)
	}

	// Verify the file was created
	fileContent, err := os.ReadFile(newFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	assert.Equal(t, content, fileContent, "File content should match")

	// Test Rollback operation
	err = session.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	// Verify the file was removed
	_, err = os.Stat(newFile)
	assert.True(t, os.IsNotExist(err), "File should be removed after rollback")
}

func TestSession_Delete(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "patch-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a test file
	testFile := filepath.Join(tempDir, "test.txt")
	originalContent := []byte("original content\n")
	err = os.WriteFile(testFile, originalContent, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create a new session
	session, err := NewSession()
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	// Test Delete operation
	err = session.Delete(testFile)
	if err != nil {
		t.Fatalf("Failed to delete file: %v", err)
	}

	// Verify the file was deleted
	_, err = os.Stat(testFile)
	assert.True(t, os.IsNotExist(err), "File should be deleted")

	// Test Rollback operation
	err = session.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	// Verify the file was restored
	content, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	assert.Equal(t, originalContent, content, "File should be restored with original content")
}

func TestSession_Move(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "patch-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a test file
	srcFile := filepath.Join(tempDir, "src.txt")
	originalContent := []byte("original content\n")
	err = os.WriteFile(srcFile, originalContent, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create a new session
	session, err := NewSession()
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	// Test Move operation
	dstFile := filepath.Join(tempDir, "dst.txt")
	err = session.Move(srcFile, dstFile)
	if err != nil {
		t.Fatalf("Failed to move file: %v", err)
	}

	// Verify the file was moved
	_, err = os.Stat(srcFile)
	assert.True(t, os.IsNotExist(err), "Source file should not exist")

	content, err := os.ReadFile(dstFile)
	if err != nil {
		t.Fatalf("Failed to read destination file: %v", err)
	}
	assert.Equal(t, originalContent, content, "Destination file content should match original")

	// Test Rollback operation
	err = session.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	// Verify the file was moved back
	_, err = os.Stat(dstFile)
	assert.True(t, os.IsNotExist(err), "Destination file should not exist after rollback")

	content, err = os.ReadFile(srcFile)
	if err != nil {
		t.Fatalf("Failed to read source file: %v", err)
	}
	assert.Equal(t, originalContent, content, "Source file should be restored with original content")
}

func TestGenerateDiff(t *testing.T) {
	oldContent := []byte("line1\nline2\nline3\n")
	newContent := []byte("line1\nmodified line2\nline3\nline4\n")

	diff, err := GenerateDiff(oldContent, newContent, "test.txt", 3)
	if err != nil {
		t.Fatalf("Failed to generate diff: %v", err)
	}

	// Verify diff stats
	assert.Equal(t, 1, diff.Stats.FilesChanged, "Should have 1 file changed")
	assert.Equal(t, 2, diff.Stats.Insertions, "Should have 2 insertions")
	assert.Equal(t, 1, diff.Stats.Deletions, "Should have 1 deletion")
	assert.Equal(t, 1, diff.Stats.Hunks, "Should have 1 hunk")

	// Verify diff contains expected changes
	assert.Contains(t, diff.Patch, "modified line2", "Diff should contain modified line")
	assert.Contains(t, diff.Patch, "line4", "Diff should contain new line")
}

func TestSession_ApplyPatch(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "patch-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a test file
	testFile := filepath.Join(tempDir, "test.txt")
	originalContent := []byte("line1\nline2\nline3\n")
	err = os.WriteFile(testFile, originalContent, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Generate a diff
	newContent := []byte("line1\nmodified line2\nline3\nline4\n")
	diff, err := GenerateDiff(originalContent, newContent, testFile, 3)
	if err != nil {
		t.Fatalf("Failed to generate diff: %v", err)
	}

	// Create a new session
	session, err := NewSession()
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	// Apply the patch
	err = session.ApplyPatch(diff.Patch)
	if err != nil {
		t.Fatalf("Failed to apply patch: %v", err)
	}

	// Verify the file was updated
	content, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	assert.Equal(t, newContent, content, "File content should be updated according to the patch")

	// Test Rollback operation
	err = session.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	// Verify the file was restored to original content
	content, err = os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	assert.Equal(t, originalContent, content, "File content should be restored to original")
}
