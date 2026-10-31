package metadata

import "testing"

// TestNormalizePath verifies canonicalization and rejection of relative paths.
func TestNormalizePath(t *testing.T) {
	cases := map[string]string{
		"/":       "/",
		"/a/b/":   "/a/b",
		"//a//b":  "/a/b",
		"/a/./b":  "/a/b",
		"/a/../b": "/b",
	}
	for input, want := range cases {
		got, err := NormalizePath(input)
		if err != nil {
			t.Fatalf("%q: %v", input, err)
		}
		if got != want {
			t.Fatalf("%q: got %q want %q", input, got, want)
		}
	}
	for _, input := range []string{"", "relative", "/a\x00b"} {
		if _, err := NormalizePath(input); err == nil {
			t.Fatalf("%q: expected error", input)
		}
	}
}

// TestPathRelations verifies parent, base, descendant, and child-prefix rules.
func TestPathRelations(t *testing.T) {
	if Parent("/a/b") != "/a" || Parent("/") != "" {
		t.Fatal("parent mismatch")
	}
	if Base("/a/b") != "b" || Base("/") != "/" {
		t.Fatal("base mismatch")
	}
	if !IsDescendant("/a/b", "/a") || IsDescendant("/ab", "/a") || IsDescendant("/a", "/") == false {
		t.Fatal("descendant mismatch")
	}
	if ChildPrefix("/") != "/" || ChildPrefix("/a") != "/a/" {
		t.Fatal("child prefix mismatch")
	}
}
