package main

import (
	"os"
	"testing"
	"time"
)

func TestBuildReportDedupeDay(t *testing.T) {
	csvData := "scholar_id,contact_date,channel,program,status\n" +
		"S-1,2026-01-01,Email,Alpha,Reached\n" +
		"S-1,2026-01-01,SMS,Alpha,Reached\n" +
		"S-1,2026-01-10,Call,Alpha,Reached\n"

	file, err := os.CreateTemp(t.TempDir(), "touchpoints-*.csv")
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}
	if _, err := file.WriteString(csvData); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close csv: %v", err)
	}

	asOf := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	report, err := buildReport(file.Name(), asOf, 30, 15, 5, true)
	if err != nil {
		t.Fatalf("build report dedupe: %v", err)
	}
	if len(report.Scholars) != 1 {
		t.Fatalf("expected 1 scholar, got %d", len(report.Scholars))
	}
	if report.Scholars[0].ContactCount != 2 {
		t.Fatalf("expected deduped contact count 2, got %d", report.Scholars[0].ContactCount)
	}
	if !floatEqual(report.Scholars[0].AvgIntervalDays, 9.0) {
		t.Fatalf("expected avg interval 9.0, got %.1f", report.Scholars[0].AvgIntervalDays)
	}

	reportRaw, err := buildReport(file.Name(), asOf, 30, 15, 5, false)
	if err != nil {
		t.Fatalf("build report raw: %v", err)
	}
	if len(reportRaw.Scholars) != 1 {
		t.Fatalf("expected 1 scholar, got %d", len(reportRaw.Scholars))
	}
	if reportRaw.Scholars[0].ContactCount != 3 {
		t.Fatalf("expected raw contact count 3, got %d", reportRaw.Scholars[0].ContactCount)
	}
	if !floatEqual(reportRaw.Scholars[0].AvgIntervalDays, 4.5) {
		t.Fatalf("expected avg interval 4.5, got %.1f", reportRaw.Scholars[0].AvgIntervalDays)
	}
}

func TestBuildReportDueSummary(t *testing.T) {
	csvData := "scholar_id,contact_date,channel,program,status\n" +
		"S-1,2025-11-06,Email,Alpha,Reached\n" + // due_0_7 (next due 2026-02-04)
		"S-2,2025-11-12,Email,Alpha,Reached\n" + // due_8_14 (next due 2026-02-10)
		"S-3,2025-11-20,Email,Alpha,Reached\n" + // due_15_30 (next due 2026-02-18)
		"S-4,2025-12-10,Email,Alpha,Reached\n" + // due_31_60 (next due 2026-03-10)
		"S-5,2026-01-05,Email,Alpha,Reached\n" + // due_61_plus (next due 2026-04-05)
		"S-6,2025-10-01,Email,Alpha,Reached\n" // overdue (next due 2025-12-30)

	file, err := os.CreateTemp(t.TempDir(), "touchpoints-*.csv")
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}
	if _, err := file.WriteString(csvData); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close csv: %v", err)
	}

	asOf := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	report, err := buildReport(file.Name(), asOf, 90, 45, 5, false)
	if err != nil {
		t.Fatalf("build report: %v", err)
	}

	bucketCounts := map[string]int{}
	for _, entry := range report.DueSummary {
		bucketCounts[entry.Label] = entry.Count
	}

	expect := map[string]int{
		"overdue":     1,
		"due_0_7":     1,
		"due_8_14":    1,
		"due_15_30":   1,
		"due_31_60":   1,
		"due_61_plus": 1,
	}
	for label, count := range expect {
		if bucketCounts[label] != count {
			t.Fatalf("bucket %s expected %d, got %d", label, count, bucketCounts[label])
		}
	}
}

func floatEqual(a float64, b float64) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff < 0.01
}
