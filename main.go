package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	defaultCadenceDays = 30
	defaultTopN        = 10
)

type ScholarStats struct {
	ScholarID    string
	Program      string
	LastChannel  string
	LastStatus   string
	LastContact  time.Time
	ContactCount int
	FirstContact time.Time
	Channels     map[string]int
	Contacts     []time.Time
	ContactDates map[string]struct{}
}

type ScholarSummary struct {
	ScholarID        string    `json:"scholar_id"`
	Program          string    `json:"program"`
	LastChannel      string    `json:"last_channel"`
	LastStatus       string    `json:"last_status"`
	LastContact      time.Time `json:"last_contact"`
	FirstContact     time.Time `json:"first_contact"`
	NextDueDate      time.Time `json:"next_due_date"`
	ContactCount     int       `json:"contact_count"`
	GapDays          int       `json:"gap_days"`
	DaysPastDue      int       `json:"days_past_due"`
	MissedCadences   int       `json:"missed_cadences"`
	DaysSinceFirst   int       `json:"days_since_first_contact"`
	AvgIntervalDays  float64   `json:"avg_interval_days"`
	ContactsPerMonth float64   `json:"contacts_per_month"`
	Tier             string    `json:"tier"`
}

type ProgramSummary struct {
	Program           string  `json:"program"`
	Scholars          int     `json:"scholars"`
	AvgGapDays        float64 `json:"avg_gap_days"`
	AvgMissedCadences float64 `json:"avg_missed_cadences"`
	OverdueCount      int     `json:"overdue_count"`
	CriticalCount     int     `json:"critical_count"`
	OnTrackCount      int     `json:"on_track_count"`
	DueSoonCount      int     `json:"due_soon_count"`
}

type ReportSummary struct {
	AsOf              string  `json:"as_of"`
	CadenceDays       int     `json:"cadence_days"`
	DueWindowDays     int     `json:"due_window_days"`
	TotalScholars     int     `json:"total_scholars"`
	AvgGapDays        float64 `json:"avg_gap_days"`
	MedianGapDays     float64 `json:"median_gap_days"`
	MaxGapDays        int     `json:"max_gap_days"`
	AvgMissedCadences float64 `json:"avg_missed_cadences"`
	MaxMissedCadences int     `json:"max_missed_cadences"`
	OnTrackCount      int     `json:"on_track_count"`
	DueSoonCount      int     `json:"due_soon_count"`
	OverdueCount      int     `json:"overdue_count"`
	CriticalCount     int     `json:"critical_count"`
	InvalidRows       int     `json:"invalid_rows"`
	FutureRows        int     `json:"future_rows"`
}

type Report struct {
	Summary        ReportSummary      `json:"summary"`
	ProgramSummary []ProgramSummary   `json:"program_summary"`
	ChannelSummary map[string]int     `json:"last_channel_summary"`
	StatusSummary  map[string]int     `json:"last_status_summary"`
	DueSummary     []DueBucketSummary `json:"due_summary"`
	RecencySummary []RecencyBucket    `json:"recency_summary"`
	TopGaps        []ScholarSummary   `json:"top_gaps"`
	Scholars       []ScholarSummary   `json:"scholars"`
}

type DueBucketSummary struct {
	Label   string `json:"label"`
	MinDays *int   `json:"min_days,omitempty"`
	MaxDays *int   `json:"max_days,omitempty"`
	Count   int    `json:"count"`
}

type RecencyBucket struct {
	Label   string `json:"label"`
	MinDays *int   `json:"min_days,omitempty"`
	MaxDays *int   `json:"max_days,omitempty"`
	Count   int    `json:"count"`
}

type DBConfig struct {
	URL    string
	Schema string
	Tag    string
}

func main() {
	inputPath := flag.String("input", "", "Path to outreach CSV")
	cadenceDays := flag.Int("cadence", defaultCadenceDays, "Expected cadence in days")
	asOf := flag.String("as-of", "", "Report as-of date (YYYY-MM-DD)")
	dueWindow := flag.Int("due-window", 0, "Days after cadence before overdue; default cadence/2")
	topN := flag.Int("top", defaultTopN, "Top N largest gaps to show")
	dedupeDay := flag.Bool("dedupe-day", false, "Deduplicate multiple contacts on the same day per scholar")
	jsonOut := flag.String("json", "", "Optional JSON output path")
	alertsOut := flag.String("alerts", "", "Optional CSV output for alert tiers")
	programsOut := flag.String("programs-csv", "", "Optional CSV output for program summary")
	channelsOut := flag.String("channels-csv", "", "Optional CSV output for channel summary")
	statusesOut := flag.String("statuses-csv", "", "Optional CSV output for last status summary")
	dueOut := flag.String("due-csv", "", "Optional CSV output for due-date buckets")
	recencyOut := flag.String("recency-csv", "", "Optional CSV output for recency buckets")
	minTier := flag.String("min-tier", "overdue", "Minimum tier for alerts (due_soon, overdue, critical)")
	dbEnabled := flag.Bool("db", false, "Store report in Postgres (requires TOUCHPOINT_GAP_AUDIT_DB_URL or DATABASE_URL)")
	dbSchema := flag.String("db-schema", "touchpoint_gap_audit", "Postgres schema for audit tables")
	dbTag := flag.String("db-tag", "", "Optional label for this audit run")
	initDB := flag.Bool("init-db", false, "Initialize database schema and seed data if empty")
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

	report, err := buildReport(*inputPath, asOfDate, *cadenceDays, dueWindowDays, *topN, *dedupeDay)
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

	if *alertsOut != "" {
		if err := writeAlertsCSV(report, *alertsOut, *minTier); err != nil {
			exitWithError(err)
		}
		fmt.Printf("Alert CSV saved to %s\n", *alertsOut)
	}
	if *programsOut != "" {
		if err := writeProgramCSV(report, *programsOut); err != nil {
			exitWithError(err)
		}
		fmt.Printf("Program summary CSV saved to %s\n", *programsOut)
	}
	if *channelsOut != "" {
		if err := writeChannelCSV(report, *channelsOut); err != nil {
			exitWithError(err)
		}
		fmt.Printf("Channel summary CSV saved to %s\n", *channelsOut)
	}
	if *statusesOut != "" {
		if err := writeStatusCSV(report, *statusesOut); err != nil {
			exitWithError(err)
		}
		fmt.Printf("Status summary CSV saved to %s\n", *statusesOut)
	}
	if *dueOut != "" {
		if err := writeDueCSV(report, *dueOut); err != nil {
			exitWithError(err)
		}
		fmt.Printf("Due summary CSV saved to %s\n", *dueOut)
	}
	if *recencyOut != "" {
		if err := writeRecencyCSV(report, *recencyOut); err != nil {
			exitWithError(err)
		}
		fmt.Printf("Recency summary CSV saved to %s\n", *recencyOut)
	}

	if *dbEnabled || *initDB {
		dbURL := dbURLFromEnv()
		if dbURL == "" {
			exitWithError(errors.New("database URL missing; set TOUCHPOINT_GAP_AUDIT_DB_URL or DATABASE_URL"))
		}
		cfg := DBConfig{
			URL:    dbURL,
			Schema: *dbSchema,
			Tag:    *dbTag,
		}
		seeded := false
		if *initDB {
			runID, err := seedDatabase(report, cfg)
			if err != nil {
				exitWithError(err)
			}
			if runID != "" {
				seeded = true
				fmt.Printf("\nSeeded Postgres with initial audit run (run_id=%s)\n", runID)
			}
		}
		if *dbEnabled {
			if seeded {
				fmt.Println("Skipped duplicate insert; current report already used for seed.")
			} else {
				runID, err := storeReportInDB(report, cfg)
				if err != nil {
					exitWithError(err)
				}
				fmt.Printf("\nStored audit run in Postgres (run_id=%s)\n", runID)
			}
		}
	}
}

func buildReport(path string, asOf time.Time, cadenceDays int, dueWindowDays int, topN int, dedupeDay bool) (Report, error) {
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
	futureRows := 0
	asOfDate := dateOnly(asOf)

	for {
		record, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
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
		if parsedDate.After(asOfDate) {
			futureRows++
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
		if program != "" && scholar.Program == "" {
			scholar.Program = program
		}
		if dedupeDay {
			if scholar.ContactDates == nil {
				scholar.ContactDates = map[string]struct{}{}
			}
			dateKey := dateOnly(parsedDate).Format("2006-01-02")
			if _, seen := scholar.ContactDates[dateKey]; seen {
				if scholar.LastContact.IsZero() || parsedDate.After(scholar.LastContact) || parsedDate.Equal(scholar.LastContact) {
					scholar.LastContact = parsedDate
					scholar.LastChannel = channel
					scholar.LastStatus = status
				}
				continue
			}
			scholar.ContactDates[dateKey] = struct{}{}
		}
		scholar.ContactCount++
		scholar.Contacts = append(scholar.Contacts, parsedDate)
		if !scholar.FirstContact.IsZero() {
			if parsedDate.Before(scholar.FirstContact) {
				scholar.FirstContact = parsedDate
			}
		} else {
			scholar.FirstContact = parsedDate
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
	missedCadencesTotal := 0
	maxMissedCadences := 0
	channelSummary := map[string]int{}
	statusSummary := map[string]int{}
	programBuckets := map[string][]ScholarSummary{}

	for _, scholar := range stats {
		gap := gapDays(asOf, scholar.LastContact)
		missedCadencesValue := missedCadences(gap, cadenceDays)
		tier := gapTier(gap, cadenceDays, dueWindowDays)
		nextDueDate := time.Time{}
		daysPastDue := 0
		daysSinceFirst := 0
		avgInterval := 0.0
		contactsPerMonthRate := 0.0
		if !scholar.LastContact.IsZero() {
			nextDueDate = dateOnly(scholar.LastContact.AddDate(0, 0, cadenceDays))
			if gap > cadenceDays {
				daysPastDue = gap - cadenceDays
			}
		}
		if !scholar.FirstContact.IsZero() {
			daysSinceFirst = gapDays(asOf, scholar.FirstContact)
			avgInterval = averageIntervalDays(scholar.Contacts)
			contactsPerMonthRate = contactsPerMonth(scholar.ContactCount, daysSinceFirst)
		}
		summary := ScholarSummary{
			ScholarID:        scholar.ScholarID,
			Program:          scholar.Program,
			LastChannel:      scholar.LastChannel,
			LastStatus:       scholar.LastStatus,
			LastContact:      scholar.LastContact,
			FirstContact:     scholar.FirstContact,
			NextDueDate:      nextDueDate,
			ContactCount:     scholar.ContactCount,
			GapDays:          gap,
			DaysPastDue:      daysPastDue,
			MissedCadences:   missedCadencesValue,
			DaysSinceFirst:   daysSinceFirst,
			AvgIntervalDays:  avgInterval,
			ContactsPerMonth: contactsPerMonthRate,
			Tier:             tier,
		}
		summaries = append(summaries, summary)
		gapValues = append(gapValues, gap)
		missedCadencesTotal += missedCadencesValue
		if missedCadencesValue > maxMissedCadences {
			maxMissedCadences = missedCadencesValue
		}
		if summary.LastChannel != "" {
			channelSummary[summary.LastChannel]++
		}
		statusKey := strings.TrimSpace(summary.LastStatus)
		if statusKey == "" {
			statusKey = "Unknown"
		}
		statusSummary[statusKey]++
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
	avgMissedCadences := 0.0
	if len(summaries) > 0 {
		avgMissedCadences = round1(float64(missedCadencesTotal) / float64(len(summaries)))
	}

	onTrack, dueSoon, overdue, critical := countTiers(summaries)

	report := Report{
		Summary: ReportSummary{
			AsOf:              asOf.Format("2006-01-02"),
			CadenceDays:       cadenceDays,
			DueWindowDays:     dueWindowDays,
			TotalScholars:     len(summaries),
			AvgGapDays:        avgGap,
			MedianGapDays:     medianGap,
			MaxGapDays:        maxGap,
			AvgMissedCadences: avgMissedCadences,
			MaxMissedCadences: maxMissedCadences,
			OnTrackCount:      onTrack,
			DueSoonCount:      dueSoon,
			OverdueCount:      overdue,
			CriticalCount:     critical,
			InvalidRows:       invalidRows,
			FutureRows:        futureRows,
		},
		ProgramSummary: programSummary,
		ChannelSummary: channelSummary,
		StatusSummary:  statusSummary,
		DueSummary:     buildDueSummary(summaries, asOfDate),
		RecencySummary: buildRecencySummary(summaries),
		TopGaps:        topGaps,
		Scholars:       summaries,
	}

	return report, nil
}

func buildProgramSummary(buckets map[string][]ScholarSummary) []ProgramSummary {
	result := make([]ProgramSummary, 0, len(buckets))
	for program, entries := range buckets {
		gaps := make([]int, 0, len(entries))
		programSummary := ProgramSummary{Program: program, Scholars: len(entries)}
		missedTotal := 0
		for _, entry := range entries {
			gaps = append(gaps, entry.GapDays)
			missedTotal += entry.MissedCadences
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
		if programSummary.Scholars > 0 {
			programSummary.AvgMissedCadences = round1(float64(missedTotal) / float64(programSummary.Scholars))
		}
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

func averageIntervalDays(dates []time.Time) float64 {
	if len(dates) < 2 {
		return 0
	}
	normalized := make([]time.Time, 0, len(dates))
	for _, value := range dates {
		if value.IsZero() {
			continue
		}
		normalized = append(normalized, dateOnly(value))
	}
	if len(normalized) < 2 {
		return 0
	}
	sort.Slice(normalized, func(i, j int) bool {
		return normalized[i].Before(normalized[j])
	})
	totalDays := 0
	for idx := 1; idx < len(normalized); idx++ {
		diff := normalized[idx].Sub(normalized[idx-1])
		totalDays += int(diff.Hours() / 24)
	}
	intervals := len(normalized) - 1
	if intervals == 0 {
		return 0
	}
	return round1(float64(totalDays) / float64(intervals))
}

func contactsPerMonth(contactCount int, daysSinceFirst int) float64 {
	if contactCount <= 0 || daysSinceFirst <= 0 {
		return 0
	}
	return round1(float64(contactCount) / float64(daysSinceFirst) * 30.0)
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

func missedCadences(gap int, cadenceDays int) int {
	if cadenceDays <= 0 {
		return 0
	}
	if gap <= cadenceDays {
		return 0
	}
	return (gap-cadenceDays+cadenceDays-1)/cadenceDays
}

func printReport(report Report, inputPath string) {
	fmt.Println("Group Scholar Touchpoint Gap Audit")
	fmt.Println(strings.Repeat("=", 38))
	fmt.Printf("Input: %s\n", filepath.Base(inputPath))
	fmt.Printf("As of: %s\n", report.Summary.AsOf)
	fmt.Printf("Cadence: %d days (due window %d days)\n", report.Summary.CadenceDays, report.Summary.DueWindowDays)
	fmt.Printf("Total scholars: %d\n", report.Summary.TotalScholars)
	fmt.Printf("Gap avg/median/max: %.1f / %.1f / %d days\n", report.Summary.AvgGapDays, report.Summary.MedianGapDays, report.Summary.MaxGapDays)
	fmt.Printf("Missed cadences avg/max: %.1f / %d\n", report.Summary.AvgMissedCadences, report.Summary.MaxMissedCadences)
	fmt.Printf("On track: %d | Due soon: %d | Overdue: %d | Critical: %d\n", report.Summary.OnTrackCount, report.Summary.DueSoonCount, report.Summary.OverdueCount, report.Summary.CriticalCount)
	if report.Summary.InvalidRows > 0 {
		fmt.Printf("Invalid rows skipped: %d\n", report.Summary.InvalidRows)
	}
	if report.Summary.FutureRows > 0 {
		fmt.Printf("Future-dated rows ignored: %d\n", report.Summary.FutureRows)
	}
	if len(report.DueSummary) > 0 {
		fmt.Printf("Due buckets: %s\n", formatDueSummary(report.DueSummary))
	}
	if len(report.RecencySummary) > 0 {
		fmt.Printf("Recency buckets: %s\n", formatRecencySummary(report.RecencySummary))
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
			fmt.Printf("%s | scholars %d | avg gap %.1f | avg missed %.1f | on track %d | due soon %d | overdue %d | critical %d\n",
				entry.Program,
				entry.Scholars,
				entry.AvgGapDays,
				entry.AvgMissedCadences,
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

	if len(report.StatusSummary) > 0 {
		fmt.Println("\nLast status summary")
		fmt.Println(strings.Repeat("-", 38))
		statuses := make([]string, 0, len(report.StatusSummary))
		for status := range report.StatusSummary {
			statuses = append(statuses, status)
		}
		sort.Strings(statuses)
		for _, status := range statuses {
			fmt.Printf("%s: %d\n", status, report.StatusSummary[status])
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

func dbURLFromEnv() string {
	if value := strings.TrimSpace(os.Getenv("TOUCHPOINT_GAP_AUDIT_DB_URL")); value != "" {
		return value
	}
	return strings.TrimSpace(os.Getenv("DATABASE_URL"))
}

func sanitizeSchema(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", errors.New("db schema is required")
	}
	valid := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	if !valid.MatchString(value) {
		return "", fmt.Errorf("invalid schema name: %s", value)
	}
	return value, nil
}

func seedDatabase(report Report, cfg DBConfig) (string, error) {
	schema, err := sanitizeSchema(cfg.Schema)
	if err != nil {
		return "", err
	}

	db, err := sql.Open("pgx", cfg.URL)
	if err != nil {
		return "", err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return "", err
	}

	if err := ensureSchema(ctx, db, schema); err != nil {
		return "", err
	}

	var count int
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.audit_runs`, schema)).Scan(&count); err != nil {
		return "", err
	}
	if count > 0 {
		fmt.Println("Audit data already present; skipping seed.")
		return "", nil
	}

	return storeReportTx(ctx, db, report, schema, cfg.Tag)
}

func storeReportInDB(report Report, cfg DBConfig) (string, error) {
	schema, err := sanitizeSchema(cfg.Schema)
	if err != nil {
		return "", err
	}

	db, err := sql.Open("pgx", cfg.URL)
	if err != nil {
		return "", err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return "", err
	}

	if err := ensureSchema(ctx, db, schema); err != nil {
		return "", err
	}

	return storeReportTx(ctx, db, report, schema, cfg.Tag)
}

func storeReportTx(ctx context.Context, db *sql.DB, report Report, schema string, tag string) (string, error) {
	runID := uuid.New()
	asOfDate, err := parseDate(report.Summary.AsOf)
	if err != nil {
		return "", err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	_, err = tx.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s.audit_runs (
			id, as_of, cadence_days, due_window_days, total_scholars,
			avg_gap_days, median_gap_days, max_gap_days, avg_missed_cadences,
			max_missed_cadences, on_track_count, due_soon_count, overdue_count,
			critical_count, invalid_rows, future_rows, run_tag
		) VALUES (
			$1,$2,$3,$4,$5,
			$6,$7,$8,$9,$10,
			$11,$12,$13,$14,
			$15,$16,$17,$18
		)`, schema),
		runID,
		dateOnly(asOfDate),
		report.Summary.CadenceDays,
		report.Summary.DueWindowDays,
		report.Summary.TotalScholars,
		report.Summary.AvgGapDays,
		report.Summary.MedianGapDays,
		report.Summary.MaxGapDays,
		report.Summary.AvgMissedCadences,
		report.Summary.MaxMissedCadences,
		report.Summary.OnTrackCount,
		report.Summary.DueSoonCount,
		report.Summary.OverdueCount,
		report.Summary.CriticalCount,
		report.Summary.InvalidRows,
		report.Summary.FutureRows,
		nullString(tag),
	)
	if err != nil {
		_ = tx.Rollback()
		return "", err
	}

	insertScholarSQL := fmt.Sprintf(`
		INSERT INTO %s.audit_scholar_gaps (
			id, run_id, scholar_id, program, last_channel, last_status,
			last_contact, first_contact, next_due_date, contact_count, gap_days, days_past_due,
			missed_cadences, days_since_first_contact, avg_interval_days, contacts_per_month, tier
		) VALUES (
			$1,$2,$3,$4,$5,$6,
			$7,$8,$9,$10,$11,$12,
			$13,$14,$15,$16,$17
		)`, schema)

	for _, entry := range report.Scholars {
		lastContact := nullDate(entry.LastContact)
		firstContact := nullDate(entry.FirstContact)
		nextDue := nullDate(entry.NextDueDate)
		_, err = tx.ExecContext(ctx, insertScholarSQL,
			uuid.New(),
			runID,
			entry.ScholarID,
			nullString(entry.Program),
			nullString(entry.LastChannel),
			nullString(entry.LastStatus),
			lastContact,
			firstContact,
			nextDue,
			entry.ContactCount,
			entry.GapDays,
			entry.DaysPastDue,
			entry.MissedCadences,
			entry.DaysSinceFirst,
			entry.AvgIntervalDays,
			entry.ContactsPerMonth,
			entry.Tier,
		)
		if err != nil {
			_ = tx.Rollback()
			return "", err
		}
	}

	insertProgramSQL := fmt.Sprintf(`
		INSERT INTO %s.audit_program_summary (
			id, run_id, program, scholars, avg_gap_days, avg_missed_cadences,
			on_track_count, due_soon_count, overdue_count, critical_count
		) VALUES (
			$1,$2,$3,$4,$5,$6,
			$7,$8,$9,$10
		)`, schema)

	for _, entry := range report.ProgramSummary {
		_, err = tx.ExecContext(ctx, insertProgramSQL,
			uuid.New(),
			runID,
			entry.Program,
			entry.Scholars,
			entry.AvgGapDays,
			entry.AvgMissedCadences,
			entry.OnTrackCount,
			entry.DueSoonCount,
			entry.OverdueCount,
			entry.CriticalCount,
		)
		if err != nil {
			_ = tx.Rollback()
			return "", err
		}
	}

	insertChannelSQL := fmt.Sprintf(`
		INSERT INTO %s.audit_channel_summary (
			id, run_id, channel, touchpoint_count
		) VALUES (
			$1,$2,$3,$4
		)`, schema)

	for channel, count := range report.ChannelSummary {
		_, err = tx.ExecContext(ctx, insertChannelSQL,
			uuid.New(),
			runID,
			channel,
			count,
		)
		if err != nil {
			_ = tx.Rollback()
			return "", err
		}
	}

	insertStatusSQL := fmt.Sprintf(`
		INSERT INTO %s.audit_status_summary (
			id, run_id, status, touchpoint_count
		) VALUES (
			$1,$2,$3,$4
		)`, schema)

	for status, count := range report.StatusSummary {
		_, err = tx.ExecContext(ctx, insertStatusSQL,
			uuid.New(),
			runID,
			status,
			count,
		)
		if err != nil {
			_ = tx.Rollback()
			return "", err
		}
	}

	insertRecencySQL := fmt.Sprintf(`
		INSERT INTO %s.audit_recency_summary (
			id, run_id, label, min_days, max_days, bucket_count
		) VALUES (
			$1,$2,$3,$4,$5,$6
		)`, schema)

	for _, entry := range report.RecencySummary {
		_, err = tx.ExecContext(ctx, insertRecencySQL,
			uuid.New(),
			runID,
			entry.Label,
			nullInt(entry.MinDays),
			nullInt(entry.MaxDays),
			entry.Count,
		)
		if err != nil {
			_ = tx.Rollback()
			return "", err
		}
	}

	if err := tx.Commit(); err != nil {
		return "", err
	}
	return runID.String(), nil
}

func ensureSchema(ctx context.Context, db *sql.DB, schema string) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schema)); err != nil {
		return err
	}

	_, err := db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.audit_runs (
			id uuid PRIMARY KEY,
			as_of date NOT NULL,
			cadence_days integer NOT NULL,
			due_window_days integer NOT NULL,
			total_scholars integer NOT NULL,
			avg_gap_days numeric(8,2) NOT NULL,
			median_gap_days numeric(8,2) NOT NULL,
			max_gap_days integer NOT NULL,
			avg_missed_cadences numeric(8,2) NOT NULL DEFAULT 0,
			max_missed_cadences integer NOT NULL DEFAULT 0,
			on_track_count integer NOT NULL,
			due_soon_count integer NOT NULL,
			overdue_count integer NOT NULL,
			critical_count integer NOT NULL,
			invalid_rows integer NOT NULL,
			future_rows integer NOT NULL DEFAULT 0,
			run_tag text,
			created_at timestamptz NOT NULL DEFAULT now()
		)`, schema))
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		ALTER TABLE %s.audit_runs
		ADD COLUMN IF NOT EXISTS future_rows integer NOT NULL DEFAULT 0
	`, schema))
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		ALTER TABLE %s.audit_runs
		ADD COLUMN IF NOT EXISTS avg_missed_cadences numeric(8,2) NOT NULL DEFAULT 0
	`, schema))
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		ALTER TABLE %s.audit_runs
		ADD COLUMN IF NOT EXISTS max_missed_cadences integer NOT NULL DEFAULT 0
	`, schema))
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.audit_scholar_gaps (
			id uuid PRIMARY KEY,
			run_id uuid NOT NULL REFERENCES %s.audit_runs(id) ON DELETE CASCADE,
			scholar_id text NOT NULL,
			program text,
			last_channel text,
			last_status text,
			last_contact date,
			first_contact date,
			next_due_date date,
			contact_count integer NOT NULL,
			gap_days integer NOT NULL,
			days_past_due integer NOT NULL,
			days_since_first_contact integer NOT NULL DEFAULT 0,
			avg_interval_days numeric(8,2) NOT NULL DEFAULT 0,
			contacts_per_month numeric(8,2) NOT NULL DEFAULT 0,
			tier text NOT NULL,
			created_at timestamptz NOT NULL DEFAULT now()
		)`, schema, schema))
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		ALTER TABLE %s.audit_scholar_gaps
		ADD COLUMN IF NOT EXISTS first_contact date
	`, schema))
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		ALTER TABLE %s.audit_scholar_gaps
		ADD COLUMN IF NOT EXISTS next_due_date date
	`, schema))
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		ALTER TABLE %s.audit_scholar_gaps
		ADD COLUMN IF NOT EXISTS days_past_due integer NOT NULL DEFAULT 0
	`, schema))
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		ALTER TABLE %s.audit_scholar_gaps
		ADD COLUMN IF NOT EXISTS days_since_first_contact integer NOT NULL DEFAULT 0
	`, schema))
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		ALTER TABLE %s.audit_scholar_gaps
		ADD COLUMN IF NOT EXISTS avg_interval_days numeric(8,2) NOT NULL DEFAULT 0
	`, schema))
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		ALTER TABLE %s.audit_scholar_gaps
		ADD COLUMN IF NOT EXISTS contacts_per_month numeric(8,2) NOT NULL DEFAULT 0
	`, schema))
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.audit_program_summary (
			id uuid PRIMARY KEY,
			run_id uuid NOT NULL REFERENCES %s.audit_runs(id) ON DELETE CASCADE,
			program text NOT NULL,
			scholars integer NOT NULL,
			avg_gap_days numeric(8,2) NOT NULL,
			on_track_count integer NOT NULL,
			due_soon_count integer NOT NULL,
			overdue_count integer NOT NULL,
			critical_count integer NOT NULL,
			created_at timestamptz NOT NULL DEFAULT now()
		)`, schema, schema))
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.audit_channel_summary (
			id uuid PRIMARY KEY,
			run_id uuid NOT NULL REFERENCES %s.audit_runs(id) ON DELETE CASCADE,
			channel text NOT NULL,
			touchpoint_count integer NOT NULL,
			created_at timestamptz NOT NULL DEFAULT now()
		)`, schema, schema))
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.audit_status_summary (
			id uuid PRIMARY KEY,
			run_id uuid NOT NULL REFERENCES %s.audit_runs(id) ON DELETE CASCADE,
			status text NOT NULL,
			touchpoint_count integer NOT NULL,
			created_at timestamptz NOT NULL DEFAULT now()
		)`, schema, schema))
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.audit_recency_summary (
			id uuid PRIMARY KEY,
			run_id uuid NOT NULL REFERENCES %s.audit_runs(id) ON DELETE CASCADE,
			label text NOT NULL,
			min_days integer,
			max_days integer,
			bucket_count integer NOT NULL,
			created_at timestamptz NOT NULL DEFAULT now()
		)`, schema, schema))
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_audit_scholar_gaps_run_idx ON %s.audit_scholar_gaps (run_id)`, schema, schema))
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_audit_scholar_gaps_tier_idx ON %s.audit_scholar_gaps (tier)`, schema, schema))
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_audit_program_summary_run_idx ON %s.audit_program_summary (run_id)`, schema, schema))
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_audit_channel_summary_run_idx ON %s.audit_channel_summary (run_id)`, schema, schema))
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_audit_status_summary_run_idx ON %s.audit_status_summary (run_id)`, schema, schema))
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_audit_recency_summary_run_idx ON %s.audit_recency_summary (run_id)`, schema, schema))
	return err
}

func nullString(value string) sql.NullString {
	if strings.TrimSpace(value) == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: value, Valid: true}
}

func nullDate(value time.Time) sql.NullTime {
	if value.IsZero() {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: dateOnly(value), Valid: true}
}

func nullInt(value *int) sql.NullInt64 {
	if value == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(*value), Valid: true}
}

func writeAlertsCSV(report Report, path string, minTier string) error {
	threshold, ok := tierRank(minTier)
	if !ok {
		return fmt.Errorf("invalid --min-tier value: %s", minTier)
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{
		"scholar_id",
		"program",
		"last_contact",
		"first_contact",
		"next_due_date",
		"gap_days",
		"days_past_due",
		"missed_cadences",
		"days_since_first_contact",
		"avg_interval_days",
		"contacts_per_month",
		"tier",
		"last_channel",
		"last_status",
		"contact_count",
	}); err != nil {
		return err
	}

	for _, entry := range report.Scholars {
		rank, _ := tierRank(entry.Tier)
		if rank < threshold {
			continue
		}
		record := []string{
			entry.ScholarID,
			entry.Program,
			formatDate(entry.LastContact),
			formatDate(entry.FirstContact),
			formatDate(entry.NextDueDate),
			fmt.Sprintf("%d", entry.GapDays),
			fmt.Sprintf("%d", entry.DaysPastDue),
			fmt.Sprintf("%d", entry.MissedCadences),
			fmt.Sprintf("%d", entry.DaysSinceFirst),
			fmt.Sprintf("%.1f", entry.AvgIntervalDays),
			fmt.Sprintf("%.1f", entry.ContactsPerMonth),
			entry.Tier,
			entry.LastChannel,
			entry.LastStatus,
			fmt.Sprintf("%d", entry.ContactCount),
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeProgramCSV(report Report, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{
		"program",
		"scholars",
		"avg_gap_days",
		"avg_missed_cadences",
		"on_track",
		"due_soon",
		"overdue",
		"critical",
	}); err != nil {
		return err
	}

	for _, entry := range report.ProgramSummary {
		record := []string{
			entry.Program,
			fmt.Sprintf("%d", entry.Scholars),
			fmt.Sprintf("%.1f", entry.AvgGapDays),
			fmt.Sprintf("%.1f", entry.AvgMissedCadences),
			fmt.Sprintf("%d", entry.OnTrackCount),
			fmt.Sprintf("%d", entry.DueSoonCount),
			fmt.Sprintf("%d", entry.OverdueCount),
			fmt.Sprintf("%d", entry.CriticalCount),
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeChannelCSV(report Report, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{
		"channel",
		"touchpoint_count",
	}); err != nil {
		return err
	}

	channels := make([]string, 0, len(report.ChannelSummary))
	for channel := range report.ChannelSummary {
		channels = append(channels, channel)
	}
	sort.Strings(channels)

	for _, channel := range channels {
		record := []string{
			channel,
			fmt.Sprintf("%d", report.ChannelSummary[channel]),
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeStatusCSV(report Report, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{
		"status",
		"touchpoint_count",
	}); err != nil {
		return err
	}

	statuses := make([]string, 0, len(report.StatusSummary))
	for status := range report.StatusSummary {
		statuses = append(statuses, status)
	}
	sort.Strings(statuses)

	for _, status := range statuses {
		record := []string{
			status,
			fmt.Sprintf("%d", report.StatusSummary[status]),
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeDueCSV(report Report, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{
		"label",
		"min_days",
		"max_days",
		"count",
	}); err != nil {
		return err
	}

	for _, entry := range report.DueSummary {
		record := []string{
			entry.Label,
			formatOptionalInt(entry.MinDays),
			formatOptionalInt(entry.MaxDays),
			fmt.Sprintf("%d", entry.Count),
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeRecencyCSV(report Report, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{
		"label",
		"min_days",
		"max_days",
		"count",
	}); err != nil {
		return err
	}

	for _, entry := range report.RecencySummary {
		record := []string{
			entry.Label,
			formatOptionalInt(entry.MinDays),
			formatOptionalInt(entry.MaxDays),
			fmt.Sprintf("%d", entry.Count),
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
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

func tierRank(value string) (int, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "due_soon":
		return 1, true
	case "overdue":
		return 2, true
	case "critical":
		return 3, true
	case "on_track":
		return 0, true
	default:
		return 0, false
	}
}

func formatDate(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.Format("2006-01-02")
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

func buildDueSummary(entries []ScholarSummary, asOf time.Time) []DueBucketSummary {
	defs := dueBucketDefinitions()
	result := make([]DueBucketSummary, len(defs))
	for idx, def := range defs {
		result[idx] = DueBucketSummary{
			Label:   def.Label,
			MinDays: def.MinDays,
			MaxDays: def.MaxDays,
		}
	}
	index := map[string]int{}
	for idx, def := range defs {
		index[def.Label] = idx
	}
	for _, entry := range entries {
		label := bucketDueLabel(entry.NextDueDate, asOf)
		if pos, ok := index[label]; ok {
			result[pos].Count++
		}
	}
	return result
}

func buildRecencySummary(entries []ScholarSummary) []RecencyBucket {
	defs := recencyBucketDefinitions()
	result := make([]RecencyBucket, len(defs))
	for idx, def := range defs {
		result[idx] = RecencyBucket{
			Label:   def.Label,
			MinDays: def.MinDays,
			MaxDays: def.MaxDays,
		}
	}
	index := map[string]int{}
	for idx, def := range defs {
		index[def.Label] = idx
	}
	for _, entry := range entries {
		label := bucketRecencyLabel(entry)
		if pos, ok := index[label]; ok {
			result[pos].Count++
		}
	}
	return result
}

type dueBucketDefinition struct {
	Label   string
	MinDays *int
	MaxDays *int
}

type recencyBucketDefinition struct {
	Label   string
	MinDays *int
	MaxDays *int
}

func dueBucketDefinitions() []dueBucketDefinition {
	return []dueBucketDefinition{
		{Label: "overdue", MaxDays: intPtr(-1)},
		{Label: "due_0_7", MinDays: intPtr(0), MaxDays: intPtr(7)},
		{Label: "due_8_14", MinDays: intPtr(8), MaxDays: intPtr(14)},
		{Label: "due_15_30", MinDays: intPtr(15), MaxDays: intPtr(30)},
		{Label: "due_31_60", MinDays: intPtr(31), MaxDays: intPtr(60)},
		{Label: "due_61_plus", MinDays: intPtr(61)},
		{Label: "unknown", MinDays: nil, MaxDays: nil},
	}
}

func recencyBucketDefinitions() []recencyBucketDefinition {
	return []recencyBucketDefinition{
		{Label: "0_7", MinDays: intPtr(0), MaxDays: intPtr(7)},
		{Label: "8_30", MinDays: intPtr(8), MaxDays: intPtr(30)},
		{Label: "31_60", MinDays: intPtr(31), MaxDays: intPtr(60)},
		{Label: "61_90", MinDays: intPtr(61), MaxDays: intPtr(90)},
		{Label: "91_180", MinDays: intPtr(91), MaxDays: intPtr(180)},
		{Label: "181_plus", MinDays: intPtr(181)},
		{Label: "unknown", MinDays: nil, MaxDays: nil},
	}
}

func bucketDueLabel(nextDue time.Time, asOf time.Time) string {
	if nextDue.IsZero() {
		return "unknown"
	}
	asOfDate := dateOnly(asOf)
	dueDate := dateOnly(nextDue)
	daysUntil := int(dueDate.Sub(asOfDate).Hours() / 24)
	switch {
	case daysUntil < 0:
		return "overdue"
	case daysUntil <= 7:
		return "due_0_7"
	case daysUntil <= 14:
		return "due_8_14"
	case daysUntil <= 30:
		return "due_15_30"
	case daysUntil <= 60:
		return "due_31_60"
	default:
		return "due_61_plus"
	}
}

func bucketRecencyLabel(entry ScholarSummary) string {
	if entry.LastContact.IsZero() {
		return "unknown"
	}
	gap := entry.GapDays
	switch {
	case gap <= 7:
		return "0_7"
	case gap <= 30:
		return "8_30"
	case gap <= 60:
		return "31_60"
	case gap <= 90:
		return "61_90"
	case gap <= 180:
		return "91_180"
	default:
		return "181_plus"
	}
}

func formatDueSummary(entries []DueBucketSummary) string {
	parts := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.Count == 0 {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s %d", entry.Label, entry.Count))
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, " | ")
}

func formatRecencySummary(entries []RecencyBucket) string {
	parts := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.Count == 0 {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s %d", entry.Label, entry.Count))
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, " | ")
}

func formatOptionalInt(value *int) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%d", *value)
}

func intPtr(value int) *int {
	return &value
}

func exitWithError(err error) {
	fmt.Fprintln(os.Stderr, "Error:", err)
	os.Exit(1)
}
