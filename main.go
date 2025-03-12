package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	defaultCadenceDays = 30
	defaultTopN        = 10
)

type ScholarStats struct {
	ScholarID   string
	Program     string
	LastChannel string
	LastStatus  string
	LastContact time.Time
	ContactCount int
	FirstContact time.Time
	Channels    map[string]int
}

type ScholarSummary struct {
	ScholarID   string    `json:"scholar_id"`
	Program     string    `json:"program"`
	LastChannel string    `json:"last_channel"`
	LastStatus  string    `json:"last_status"`
	LastContact time.Time `json:"last_contact"`
	ContactCount int      `json:"contact_count"`
	GapDays     int       `json:"gap_days"`
	Tier        string    `json:"tier"`
}

type ProgramSummary struct {
	Program        string `json:"program"`
	Scholars       int    `json:"scholars"`
	AvgGapDays     float64 `json:"avg_gap_days"`
	OverdueCount   int    `json:"overdue_count"`
	CriticalCount  int    `json:"critical_count"`
	OnTrackCount   int    `json:"on_track_count"`
	DueSoonCount   int    `json:"due_soon_count"`
}

type ReportSummary struct {
	AsOf           string  `json:"as_of"`
	CadenceDays    int     `json:"cadence_days"`
	DueWindowDays  int     `json:"due_window_days"`
	TotalScholars  int     `json:"total_scholars"`
	AvgGapDays     float64 `json:"avg_gap_days"`
	MedianGapDays  float64 `json:"median_gap_days"`
	MaxGapDays     int     `json:"max_gap_days"`
	OnTrackCount   int     `json:"on_track_count"`
	DueSoonCount   int     `json:"due_soon_count"`
	OverdueCount   int     `json:"overdue_count"`
	CriticalCount  int     `json:"critical_count"`
	InvalidRows    int     `json:"invalid_rows"`
}

type Report struct {
	Summary         ReportSummary    `json:"summary"`
	ProgramSummary  []ProgramSummary `json:"program_summary"`
	ChannelSummary  map[string]int   `json:"last_channel_summary"`
	TopGaps         []ScholarSummary `json:"top_gaps"`
	Scholars        []ScholarSummary `json:"scholars"`
}

func main() {
	inputPath := flag.String("input", "", "Path to outreach CSV")
	cadenceDays := flag.Int("cadence", defaultCadenceDays, "Expected cadence in days")
	asOf := flag.String("as-of", "", "Report as-of date (YYYY-MM-DD)")
	dueWindow := flag.Int("due-window", 0, "Days after cadence before overdue; default cadence/2")
	topN := flag.Int("top", defaultTopN, "Top N largest gaps to show")
	jsonOut := flag.String("json", "", "Optional JSON output path")
	flag.Parse()

	if *inputPath == "" {
		exitWithError(errors.New("--input is required"))
	}
	if *cadenceDays <= 0 {
		exitWithError(errors.New("--cadence must be positive"))
	}

	asOfDate := time.Now()
	if *asOf != "" {
		parsed, err := parseDate(*asOf)
		if err != nil {
			exitWithError(fmt.Errorf("invalid --as-of date: %w", err))
		}
		asOfDate = parsed
	}
	asOfDate = dateOnly(asOfDate)

	dueWindowDays := *dueWindow
	if dueWindowDays <= 0 {
		dueWindowDays = int(math.Ceil(float64(*cadenceDays) * 0.5))
	}

	report, err := buildReport(*inputPath, asOfDate, *cadenceDays, dueWindowDays, *topN)
	if err != nil {
		exitWithError(err)
	}

	printReport(report, *inputPath)

	if *jsonOut != "" {
		if err := writeJSON(report, *jsonOut); err != nil {
			exitWithError(err)
		}
		fmt.Printf("\nJSON report saved to %s\n", *jsonOut)
	}
}

func buildReport(path string, asOf time.Time, cadenceDays int, dueWindowDays int, topN int) (Report, error) {
	file, err := os.Open(path)
	if err != nil {
		return Report{}, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1

	headers, err := reader.Read()
	if err != nil {
		return Report{}, fmt.Errorf("unable to read header: %w", err)
	}

	colMap := normalizeHeaders(headers)
	idIdx, ok := findColumn(colMap, []string{"scholar_id", "scholarid", "scholar", "student_id", "studentid"})
	if !ok {
		return Report{}, errors.New("missing scholar_id column")
	}
	dateIdx, ok := findColumn(colMap, []string{"contact_date", "contacted_at", "date", "touchpoint_date", "touchpoint"})
	if !ok {
		return Report{}, errors.New("missing contact_date column")
	}
	programIdx, _ := findColumn(colMap, []string{"program", "cohort", "track"})
	channelIdx, _ := findColumn(colMap, []string{"channel", "method", "touchpoint_channel"})
	statusIdx, _ := findColumn(colMap, []string{"status", "outcome", "result"})

	stats := map[string]*ScholarStats{}
	invalidRows := 0

	for {
		record, err := reader.Read()
		if err != nil {
			if errors.Is(err, os.EOF) {
				break
			}
			return Report{}, fmt.Errorf("unable to read CSV: %w", err)
		}
		if len(record) == 0 {
			continue
		}

		scholarID := getValue(record, idIdx)
		if scholarID == "" {
			invalidRows++
			continue
		}

		dateStr := getValue(record, dateIdx)
		parsedDate, err := parseDate(dateStr)
		if err != nil {
			invalidRows++
			continue
		}

		program := ""
		if programIdx >= 0 {
			program = getValue(record, programIdx)
		}
		channel := ""
		if channelIdx >= 0 {
			channel = getValue(record, channelIdx)
		}
		status := ""
		if statusIdx >= 0 {
			status = getValue(record, statusIdx)
		}

		scholar, exists := stats[scholarID]
		if !exists {
			scholar = &ScholarStats{ScholarID: scholarID, Channels: map[string]int{}}
			stats[scholarID] = scholar
		}
		scholar.ContactCount++
		if !scholar.FirstContact.IsZero() {
			if parsedDate.Before(scholar.FirstContact) {
				scholar.FirstContact = parsedDate
			}
		} else {
			scholar.FirstContact = parsedDate
		}

		if program != "" && scholar.Program == "" {
			scholar.Program = program
		}
		if channel != "" {
			scholar.Channels[channel]++
		}
		if scholar.LastContact.IsZero() || parsedDate.After(scholar.LastContact) {
			scholar.LastContact = parsedDate
			scholar.LastChannel = channel
			scholar.LastStatus = status
		}
	}

	summaries := make([]ScholarSummary, 0, len(stats))
	gapValues := make([]int, 0, len(stats))
	channelSummary := map[string]int{}
	programBuckets := map[string][]ScholarSummary{}

	for _, scholar := range stats {
		gap := gapDays(asOf, scholar.LastContact)
		tier := gapTier(gap, cadenceDays, dueWindowDays)
		summary := ScholarSummary{
			ScholarID: scholar.ScholarID,
			Program: scholar.Program,
			LastChannel: scholar.LastChannel,
			LastStatus: scholar.LastStatus,
			LastContact: scholar.LastContact,
			ContactCount: scholar.ContactCount,
			GapDays: gap,
			Tier: tier,
		}
		summaries = append(summaries, summary)
		gapValues = append(gapValues, gap)
		if summary.LastChannel != "" {
			channelSummary[summary.LastChannel]++
		}
		programKey := summary.Program
		if programKey == "" {
			programKey = "Unassigned"
		}
		programBuckets[programKey] = append(programBuckets[programKey], summary)
	}

	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].GapDays > summaries[j].GapDays
	})

	topGaps := summaries
	if topN > 0 && len(topGaps) > topN {
		topGaps = topGaps[:topN]
	}

	programSummary := buildProgramSummary(programBuckets)
	if len(programSummary) > 1 {
		sort.Slice(programSummary, func(i, j int) bool {
			return programSummary[i].OverdueCount+programSummary[i].CriticalCount > programSummary[j].OverdueCount+programSummary[j].CriticalCount
		})
	}

	avgGap, medianGap, maxGap := summarizeGaps(gapValues)

	onTrack, dueSoon, overdue, critical := countTiers(summaries)

	report := Report{
		Summary: ReportSummary{
			AsOf: asOf.Format("2006-01-02"),
			CadenceDays: cadenceDays,
			DueWindowDays: dueWindowDays,
			TotalScholars: len(summaries),
			AvgGapDays: avgGap,
			MedianGapDays: medianGap,
			MaxGapDays: maxGap,
			OnTrackCount: onTrack,
			DueSoonCount: dueSoon,
			OverdueCount: overdue,
			CriticalCount: critical,
			InvalidRows: invalidRows,
		},
		ProgramSummary: programSummary,
		ChannelSummary: channelSummary,
		TopGaps: topGaps,
		Scholars: summaries,
	}

	return report, nil
}

func buildProgramSummary(buckets map[string][]ScholarSummary) []ProgramSummary {
	result := make([]ProgramSummary, 0, len(buckets))
	for program, entries := range buckets {
		gaps := make([]int, 0, len(entries))
		programSummary := ProgramSummary{Program: program, Scholars: len(entries)}
		for _, entry := range entries {
			gaps = append(gaps, entry.GapDays)
			switch entry.Tier {
			case "on_track":
				programSummary.OnTrackCount++
			case "due_soon":
				programSummary.DueSoonCount++
			case "overdue":
				programSummary.OverdueCount++
			case "critical":
				programSummary.CriticalCount++
			}
		}
		avgGap, _, _ := summarizeGaps(gaps)
		programSummary.AvgGapDays = avgGap
		result = append(result, programSummary)
	}
	return result
}

func summarizeGaps(gaps []int) (float64, float64, int) {
	if len(gaps) == 0 {
		return 0, 0, 0
	}
	copyValues := append([]int{}, gaps...)
	sort.Ints(copyValues)
	maxGap := copyValues[len(copyValues)-1]
	sum := 0
	for _, gap := range copyValues {
		sum += gap
	}
	avg := float64(sum) / float64(len(copyValues))
	median := 0.0
	mid := len(copyValues) / 2
	if len(copyValues)%2 == 0 {
		median = float64(copyValues[mid-1]+copyValues[mid]) / 2
	} else {
		median = float64(copyValues[mid])
	}
	return round1(avg), round1(median), maxGap
}

func round1(value float64) float64 {
	return math.Round(value*10) / 10
}

func countTiers(entries []ScholarSummary) (int, int, int, int) {
	onTrack, dueSoon, overdue, critical := 0, 0, 0, 0
	for _, entry := range entries {
		switch entry.Tier {
		case "on_track":
			onTrack++
		case "due_soon":
			dueSoon++
		case "overdue":
			overdue++
		case "critical":
			critical++
		}
	}
	return onTrack, dueSoon, overdue, critical
}

func gapDays(asOf time.Time, lastContact time.Time) int {
	if lastContact.IsZero() {
		return 0
	}
	asOfDate := dateOnly(asOf)
	lastDate := dateOnly(lastContact)
	if lastDate.After(asOfDate) {
		return 0
	}
	delta := asOfDate.Sub(lastDate)
	return int(delta.Hours() / 24)
}

func gapTier(gap int, cadenceDays int, dueWindowDays int) string {
	if gap <= cadenceDays {
		return "on_track"
	}
	if gap <= cadenceDays+dueWindowDays {
		return "due_soon"
	}
	if gap <= cadenceDays*2 {
		return "overdue"
	}
	return "critical"
}

func printReport(report Report, inputPath string) {
	fmt.Println("Group Scholar Touchpoint Gap Audit")
	fmt.Println(strings.Repeat("=", 38))
	fmt.Printf("Input: %s\n", filepath.Base(inputPath))
	fmt.Printf("As of: %s\n", report.Summary.AsOf)
	fmt.Printf("Cadence: %d days (due window %d days)\n", report.Summary.CadenceDays, report.Summary.DueWindowDays)
	fmt.Printf("Total scholars: %d\n", report.Summary.TotalScholars)
	fmt.Printf("Gap avg/median/max: %.1f / %.1f / %d days\n", report.Summary.AvgGapDays, report.Summary.MedianGapDays, report.Summary.MaxGapDays)
	fmt.Printf("On track: %d | Due soon: %d | Overdue: %d | Critical: %d\n", report.Summary.OnTrackCount, report.Summary.DueSoonCount, report.Summary.OverdueCount, report.Summary.CriticalCount)
	if report.Summary.InvalidRows > 0 {
		fmt.Printf("Invalid rows skipped: %d\n", report.Summary.InvalidRows)
	}

	fmt.Println("\nTop gaps")
	fmt.Println(strings.Repeat("-", 38))
	if len(report.TopGaps) == 0 {
		fmt.Println("No scholars found.")
	} else {
		for _, entry := range report.TopGaps {
			program := entry.Program
			if program == "" {
				program = "Unassigned"
			}
			channel := entry.LastChannel
			if channel == "" {
				channel = "Unknown"
			}
			fmt.Printf("%s | %s | gap %d days | %s | last %s via %s\n",
				entry.ScholarID,
				program,
				entry.GapDays,
				entry.Tier,
				entry.LastContact.Format("2006-01-02"),
				channel,
			)
		}
	}

	if len(report.ProgramSummary) > 0 {
		fmt.Println("\nProgram summary")
		fmt.Println(strings.Repeat("-", 38))
		for _, entry := range report.ProgramSummary {
			fmt.Printf("%s | scholars %d | avg gap %.1f | on track %d | due soon %d | overdue %d | critical %d\n",
				entry.Program,
				entry.Scholars,
				entry.AvgGapDays,
				entry.OnTrackCount,
				entry.DueSoonCount,
				entry.OverdueCount,
				entry.CriticalCount,
			)
		}
	}

	if len(report.ChannelSummary) > 0 {
		fmt.Println("\nLast channel summary")
		fmt.Println(strings.Repeat("-", 38))
		channels := make([]string, 0, len(report.ChannelSummary))
		for channel := range report.ChannelSummary {
			channels = append(channels, channel)
		}
		sort.Strings(channels)
		for _, channel := range channels {
			fmt.Printf("%s: %d\n", channel, report.ChannelSummary[channel])
		}
	}
}

func writeJSON(report Report, path string) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func parseDate(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, errors.New("empty date")
	}
	layouts := []string{
		"2006-01-02",
		"2006/01/02",
		"01/02/2006",
		"01-02-2006",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05Z07:00",
	}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported date format: %s", value)
}

func normalizeHeaders(headers []string) map[string]int {
	result := make(map[string]int, len(headers))
	for idx, header := range headers {
		normalized := normalizeHeader(header)
		if _, exists := result[normalized]; !exists {
			result[normalized] = idx
		}
	}
	return result
}

func normalizeHeader(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, " ", "")
	value = strings.ReplaceAll(value, "_", "")
	value = strings.ReplaceAll(value, "-", "")
	return value
}

func findColumn(headers map[string]int, names []string) (int, bool) {
	for _, name := range names {
		if idx, ok := headers[normalizeHeader(name)]; ok {
			return idx, true
		}
	}
	return -1, false
}

func getValue(record []string, idx int) string {
	if idx < 0 || idx >= len(record) {
		return ""
	}
	return strings.TrimSpace(record[idx])
}

func dateOnly(value time.Time) time.Time {
	if value.IsZero() {
		return value
	}
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, value.Location())
}

func exitWithError(err error) {
	fmt.Fprintln(os.Stderr, "Error:", err)
	os.Exit(1)
}
