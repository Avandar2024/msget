package main

import "testing"

func TestStringList(t *testing.T) {
	t.Parallel()
	var values stringList
	for _, value := range []string{"*.json", "", "*.bin"} {
		if err := values.Set(value); err != nil {
			t.Fatal(err)
		}
	}
	if got := values.String(); got != "*.json,*.bin" {
		t.Fatalf("String() = %q", got)
	}
}

func TestEnvOr(t *testing.T) {
	t.Setenv("MSGET_TEST_VALUE", "configured")
	if got := envOr("MSGET_TEST_VALUE", "fallback"); got != "configured" {
		t.Fatalf("envOr() = %q", got)
	}
	t.Setenv("MSGET_TEST_VALUE", "")
	if got := envOr("MSGET_TEST_VALUE", "fallback"); got != "fallback" {
		t.Fatalf("envOr() fallback = %q", got)
	}
}
