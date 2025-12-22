package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/alecthomas/kong"
	"github.com/dustin/go-humanize"
	"github.com/elee1766/gobtr/pkg/api"
	"github.com/elee1766/gobtr/pkg/btrfs"
	"github.com/elee1766/gobtr/pkg/config"
	"github.com/elee1766/gobtr/pkg/db"
	"github.com/elee1766/gobtr/pkg/fragmap"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

// CLI is the root command structure
type CLI struct {
	// Global flags
	LogLevel string `short:"l" default:"info" enum:"debug,info,warn,error" help:"Log level (debug, info, warn, error)"`

	// Subcommands
	WebUI      WebUICmd      `cmd:"" help:"Run the web UI server"`
	Subvolumes SubvolumesCmd `cmd:"" name:"subvol" help:"Subvolume operations"`
	Frag       FragCmd       `cmd:"" help:"Fragmentation analysis"`
}

// WebUICmd runs the web server with UI
type WebUICmd struct {
	Address string `short:"a" default:":8147" help:"API server address"`
}

func (c *WebUICmd) Run(cli *CLI) error {
	app := fx.New(
		fx.Provide(
			func() *config.Config {
				cfg := config.New()
				cfg.APIAddress = c.Address
				cfg.LogLevel = cli.LogLevel
				return cfg
			},
			provideLogger,
		),
		fx.WithLogger(func(log *slog.Logger) fxevent.Logger {
			return &fxevent.SlogLogger{Logger: log}
		}),
		db.Module,
		btrfs.Module,
		api.Module,
	)

	app.Run()
	return nil
}

// SubvolumesCmd contains subvolume subcommands
type SubvolumesCmd struct {
	List SubvolListCmd `cmd:"" help:"List subvolumes"`
	Show SubvolShowCmd `cmd:"" help:"Show subvolume details"`
}

// SubvolListCmd lists subvolumes
type SubvolListCmd struct {
	Path string `arg:"" help:"Path to btrfs filesystem mount point"`
}

func (c *SubvolListCmd) Run(cli *CLI) error {
	mgr := btrfs.New(makeLogger(cli.LogLevel))
	subvols, err := mgr.ListSubvolumes(c.Path)
	if err != nil {
		return fmt.Errorf("failed to list subvolumes: %w", err)
	}

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleRounded)
	t.AppendHeader(table.Row{"ID", "Gen", "Top Level", "Path", "RO"})

	for _, sv := range subvols {
		ro := ""
		if sv.IsReadonly {
			ro = "ro"
		}
		t.AppendRow(table.Row{sv.ID, sv.Gen, sv.TopLevel, sv.Path, ro})
	}
	t.Render()
	return nil
}

// SubvolShowCmd shows subvolume details
type SubvolShowCmd struct {
	Path string `arg:"" help:"Path to subvolume"`
}

func (c *SubvolShowCmd) Run(cli *CLI) error {
	mgr := btrfs.New(makeLogger(cli.LogLevel))
	sv, err := mgr.GetSubvolumeInfo(c.Path)
	if err != nil {
		return fmt.Errorf("failed to get subvolume info: %w", err)
	}

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleRounded)
	t.AppendRow(table.Row{"ID", sv.ID})
	t.AppendRow(table.Row{"Generation", sv.Gen})
	t.AppendRow(table.Row{"Top Level", sv.TopLevel})
	t.AppendRow(table.Row{"Path", sv.Path})
	t.AppendRow(table.Row{"UUID", sv.UUID})
	if sv.ParentUUID != "" && sv.ParentUUID != "00000000-0000-0000-0000-000000000000" {
		t.AppendRow(table.Row{"Parent UUID", sv.ParentUUID})
	}
	t.AppendRow(table.Row{"Readonly", sv.IsReadonly})
	if !sv.CreatedAt.IsZero() {
		t.AppendRow(table.Row{"Created", sv.CreatedAt.Format("2006-01-02 15:04:05")})
	}
	t.Render()
	return nil
}

// FragCmd contains fragmentation analysis subcommands
type FragCmd struct {
	File FragFileCmd `cmd:"" help:"Analyze file fragmentation"`
	FS   FragFSCmd   `cmd:"" name:"fs" help:"Analyze filesystem free-space fragmentation"`
}

// FragFileCmd analyzes file fragmentation
type FragFileCmd struct {
	Path    string `arg:"" help:"File or directory path to analyze"`
	Recurse bool   `short:"r" help:"Recursively analyze directory"`
	Top     int    `short:"n" default:"20" help:"Show top N most fragmented files"`
}

func (c *FragFileCmd) Run(cli *CLI) error {
	info, err := os.Stat(c.Path)
	if err != nil {
		return fmt.Errorf("stat path: %w", err)
	}

	if !info.IsDir() {
		// Single file analysis
		frag, err := fragmap.AnalyzeFileFragmentation(c.Path)
		if err != nil {
			return fmt.Errorf("analyze file: %w", err)
		}
		printFileFragInfo(frag)
		return nil
	}

	// Directory analysis
	var files []*fragmap.FileFragInfo
	walkFn := func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // Skip errors
		}
		if d.IsDir() {
			if !c.Recurse && path != c.Path {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}

		frag, err := fragmap.AnalyzeFileFragmentation(path)
		if err != nil {
			return nil // Skip files we can't analyze
		}
		files = append(files, frag)
		return nil
	}

	if err := filepath.WalkDir(c.Path, walkFn); err != nil {
		return fmt.Errorf("walk directory: %w", err)
	}

	if len(files) == 0 {
		fmt.Println("No files found to analyze")
		return nil
	}

	// Print aggregate stats
	stats := fragmap.AggregateFileFragmentation(files)

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleRounded)
	t.SetTitle("Aggregate Statistics")
	t.AppendRow(table.Row{"Total files", stats.TotalFiles})
	t.AppendRow(table.Row{"Total size", humanize.IBytes(uint64(stats.TotalBytes))})
	t.AppendRow(table.Row{"Total extents", stats.TotalExtents})
	t.AppendRow(table.Row{"Fragmented files", fmt.Sprintf("%d (%.1f%%)", stats.FragmentedFiles,
		float64(stats.FragmentedFiles)/float64(stats.TotalFiles)*100)})
	t.AppendSeparator()
	t.AppendRow(table.Row{"Avg DoF", fmt.Sprintf("%.2f", stats.AvgDoF)})
	t.AppendRow(table.Row{"Avg Frag%", fmt.Sprintf("%.1f%%", stats.AvgFragPct)})
	t.AppendRow(table.Row{"Avg Out-of-Order%", fmt.Sprintf("%.1f%%", stats.AvgOutOfOrderPct)})
	t.AppendRow(table.Row{"Max DoF", fmt.Sprintf("%.2f", stats.MaxDoF)})
	t.AppendRow(table.Row{"Max extents", stats.MaxExtents})
	t.Render()

	fmt.Println()

	// DoF Distribution table
	dist := table.NewWriter()
	dist.SetOutputMirror(os.Stdout)
	dist.SetStyle(table.StyleRounded)
	dist.SetTitle("DoF Distribution")
	dist.AppendHeader(table.Row{"Range", "Files", "Description"})
	dist.AppendRow(table.Row{"DoF = 1", stats.DoFHistogram["1"], "Ideal (no fragmentation)"})
	dist.AppendRow(table.Row{"DoF 1-2", stats.DoFHistogram["1-2"], "Minimal fragmentation"})
	dist.AppendRow(table.Row{"DoF 2-5", stats.DoFHistogram["2-5"], "Moderate fragmentation"})
	dist.AppendRow(table.Row{"DoF 5-10", stats.DoFHistogram["5-10"], "High fragmentation"})
	dist.AppendRow(table.Row{"DoF 10+", stats.DoFHistogram["10+"], "Severe fragmentation"})
	dist.Render()

	fmt.Println()

	// Print top fragmented files (only those with DoF > 1.0)
	fragmap.SortFilesByDoF(files)

	// Filter to only fragmented files
	var fragmented []*fragmap.FileFragInfo
	for _, f := range files {
		if f.DoF > 1.0 {
			fragmented = append(fragmented, f)
		}
	}

	if len(fragmented) == 0 {
		fmt.Println("\nNo fragmented files found (all files have ideal DoF = 1.0)")
	} else {
		top := table.NewWriter()
		top.SetOutputMirror(os.Stdout)
		top.SetStyle(table.StyleRounded)
		top.SetTitle(fmt.Sprintf("Top %d Most Fragmented Files", min(c.Top, len(fragmented))))
		top.AppendHeader(table.Row{"DoF", "Extents", "Frag%", "OoO%", "Size", "Path"})
		top.SetColumnConfigs([]table.ColumnConfig{
			{Number: 1, Align: text.AlignRight},
			{Number: 2, Align: text.AlignRight},
			{Number: 3, Align: text.AlignRight},
			{Number: 4, Align: text.AlignRight},
			{Number: 5, Align: text.AlignRight},
		})

		for i, f := range fragmented {
			if i >= c.Top {
				break
			}
			top.AppendRow(table.Row{
				fmt.Sprintf("%.1f", f.DoF),
				f.ExtentCount,
				fmt.Sprintf("%.1f%%", f.FragmentationPct),
				fmt.Sprintf("%.1f%%", f.OutOfOrderPct),
				humanize.IBytes(uint64(f.Size)),
				f.Path,
			})
		}
		top.Render()
	}

	return nil
}

func printFileFragInfo(f *fragmap.FileFragInfo) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleRounded)
	t.SetTitle("File Information")
	t.AppendRow(table.Row{"Path", f.Path})
	t.AppendRow(table.Row{"Size", humanize.IBytes(uint64(f.Size))})
	t.AppendRow(table.Row{"Extents", f.ExtentCount})
	t.AppendRow(table.Row{"Ideal extents", f.IdealExtents})
	t.Render()

	fmt.Println()

	m := table.NewWriter()
	m.SetOutputMirror(os.Stdout)
	m.SetStyle(table.StyleRounded)
	m.SetTitle("Fragmentation Metrics")
	m.AppendHeader(table.Row{"Metric", "Value", "Description"})
	m.AppendRow(table.Row{"Degree of Frag (DoF)", fmt.Sprintf("%.2f", f.DoF), "1.0 = ideal, higher = worse"})
	m.AppendRow(table.Row{"Fragmentation %", fmt.Sprintf("%.1f%%", f.FragmentationPct), "Discontinuities vs potential"})
	m.AppendRow(table.Row{"Out-of-Order %", fmt.Sprintf("%.1f%%", f.OutOfOrderPct), "Backwards physical jumps"})
	m.AppendSeparator()
	m.AppendRow(table.Row{"Fragmentation points", f.FragmentationPoints, ""})
	m.AppendRow(table.Row{"Backwards fragments", f.BackwardsFragments, ""})
	m.AppendRow(table.Row{"Contiguous bytes", humanize.IBytes(uint64(f.ContiguousExtentBytes)), ""})
	m.Render()
}

// FragFSCmd analyzes filesystem free-space fragmentation
type FragFSCmd struct {
	Path string `arg:"" help:"Path to btrfs filesystem mount point"`
}

func (c *FragFSCmd) Run(cli *CLI) error {
	scanner, err := fragmap.NewScanner(c.Path)
	if err != nil {
		return fmt.Errorf("create scanner: %w", err)
	}
	defer scanner.Close()

	fm, err := scanner.Scan()
	if err != nil {
		return fmt.Errorf("scan filesystem: %w", err)
	}

	// Chunk utilization (slack space)
	util := fm.CalculateChunkUtilization()
	ut := table.NewWriter()
	ut.SetOutputMirror(os.Stdout)
	ut.SetStyle(table.StyleRounded)
	ut.SetTitle("Chunk Utilization (Slack Space)")
	ut.AppendHeader(table.Row{"Type", "Allocated", "Used", "Slack", "Utilization"})
	ut.SetColumnConfigs([]table.ColumnConfig{
		{Number: 2, Align: text.AlignRight},
		{Number: 3, Align: text.AlignRight},
		{Number: 4, Align: text.AlignRight},
		{Number: 5, Align: text.AlignRight},
	})
	ut.AppendRow(table.Row{
		"Data",
		humanize.IBytes(util.DataAllocated),
		humanize.IBytes(util.DataUsed),
		humanize.IBytes(util.DataSlack),
		fmt.Sprintf("%.1f%%", util.DataUtilization),
	})
	ut.AppendRow(table.Row{
		"Metadata",
		humanize.IBytes(util.MetadataAllocated),
		humanize.IBytes(util.MetadataUsed),
		humanize.IBytes(util.MetadataSlack),
		fmt.Sprintf("%.1f%%", util.MetadataUtilization),
	})
	ut.AppendRow(table.Row{
		"System",
		humanize.IBytes(util.SystemAllocated),
		humanize.IBytes(util.SystemUsed),
		humanize.IBytes(util.SystemSlack),
		fmt.Sprintf("%.1f%%", util.SystemUtilization),
	})
	ut.AppendSeparator()
	ut.AppendRow(table.Row{
		"Total",
		humanize.IBytes(util.DataAllocated + util.MetadataAllocated + util.SystemAllocated),
		humanize.IBytes(util.DataUsed + util.MetadataUsed + util.SystemUsed),
		humanize.IBytes(util.TotalSlack),
		fmt.Sprintf("%.1f%%", util.OverallUtilization),
	})
	ut.Render()
	fmt.Println()

	for _, dev := range fm.Devices {
		blockMap, err := fm.BuildDeviceBlockMap(dev.ID)
		if err != nil {
			continue
		}
		stats := blockMap.CalculateStats()

		// Device overview
		t := table.NewWriter()
		t.SetOutputMirror(os.Stdout)
		t.SetStyle(table.StyleRounded)
		t.SetTitle(fmt.Sprintf("Device %d: %s", dev.ID, dev.Path))
		t.AppendRow(table.Row{"Total size", humanize.IBytes(stats.TotalSize)})
		t.AppendRow(table.Row{"Allocated", fmt.Sprintf("%s (%.1f%%)",
			humanize.IBytes(stats.AllocatedSize),
			float64(stats.AllocatedSize)/float64(stats.TotalSize)*100)})
		t.AppendRow(table.Row{"Free", fmt.Sprintf("%s (%.1f%%)",
			humanize.IBytes(stats.FreeSize),
			float64(stats.FreeSize)/float64(stats.TotalSize)*100)})
		t.AppendSeparator()
		t.AppendRow(table.Row{"Free regions", stats.NumFreeRegions})
		t.AppendRow(table.Row{"Largest free", fmt.Sprintf("%s (%.1f%% of free)",
			humanize.IBytes(stats.LargestFree), stats.LargestFreePct)})
		t.AppendRow(table.Row{"Avg free region", humanize.IBytes(stats.AvgFreeSize)})
		t.Render()

		fmt.Println()

		// Calculate median for display (same logic as in CalculateStats)
		// We need to show what thresholds were used
		medianFree := stats.AvgFreeSize // Approximation for display
		tinyThresh := medianFree / 10
		smallThresh := medianFree / 2

		// Fragmentation indicators
		ind := table.NewWriter()
		ind.SetOutputMirror(os.Stdout)
		ind.SetStyle(table.StyleRounded)
		ind.SetTitle("Free-Space Distribution (Adaptive Thresholds)")
		ind.AppendHeader(table.Row{"Category", "Value", "Threshold", "Status"})

		tinyStatus := statusOK()
		if stats.FreeSpaceTinyPct > 5 {
			tinyStatus = statusWarn()
		}
		ind.AppendRow(table.Row{
			"Tiny",
			fmt.Sprintf("%.1f%%", stats.FreeSpaceTinyPct),
			fmt.Sprintf("<%s", humanize.IBytes(tinyThresh)),
			tinyStatus,
		})

		smallStatus := statusOK()
		if stats.FreeSpaceSmallPct > 50 {
			smallStatus = statusWarn()
		}
		ind.AppendRow(table.Row{
			"Small",
			fmt.Sprintf("%.1f%%", stats.FreeSpaceSmallPct),
			fmt.Sprintf("<%s", humanize.IBytes(smallThresh)),
			smallStatus,
		})

		largeStatus := statusOK()
		if stats.FreeSpaceLargePct < 50 {
			largeStatus = statusWarn()
		}
		ind.AppendRow(table.Row{
			"Large",
			fmt.Sprintf("%.1f%%", stats.FreeSpaceLargePct),
			fmt.Sprintf("≥%s", humanize.IBytes(medianFree)),
			largeStatus,
		})

		ind.AppendSeparator()

		scoreStatus := "Healthy"
		switch {
		case stats.FreeSpaceFragScore >= 80:
			scoreStatus = "Severely Fragmented"
		case stats.FreeSpaceFragScore >= 50:
			scoreStatus = "Fragmented"
		case stats.FreeSpaceFragScore >= 20:
			scoreStatus = "Moderate"
		}
		ind.AppendRow(table.Row{"Unusable Score", fmt.Sprintf("%.0f/100", stats.FreeSpaceFragScore), "", scoreStatus})

		scatterStatus := "Compact"
		switch {
		case stats.ScatterScore >= 80:
			scatterStatus = "Very Scattered"
		case stats.ScatterScore >= 60:
			scatterStatus = "Scattered"
		case stats.ScatterScore >= 40:
			scatterStatus = "Moderate"
		}
		ind.AppendRow(table.Row{"Scatter Score", fmt.Sprintf("%.0f/100", stats.ScatterScore), "", scatterStatus})

		ind.Render()
		fmt.Println()
	}

	return nil
}

func statusOK() string {
	return "OK"
}

func statusWarn() string {
	return "WARNING"
}

func main() {
	cli := &CLI{}
	ctx := kong.Parse(cli,
		kong.Name("gobtr"),
		kong.Description("BTRFS management tool"),
		kong.UsageOnError(),
	)
	err := ctx.Run(cli)
	ctx.FatalIfErrorf(err)
}

func provideLogger(cfg *config.Config) *slog.Logger {
	return makeLogger(cfg.LogLevel)
}

func makeLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "info":
		lvl = slog.LevelInfo
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: lvl,
	}

	handler := slog.NewJSONHandler(os.Stdout, opts)
	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}
