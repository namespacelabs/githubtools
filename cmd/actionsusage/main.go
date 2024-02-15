package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"time"

	"github.com/google/go-github/v58/github"
)

var (
	repos    = flag.String("repos", "", "List of repositories, separated by commas. E.g. namespacelabs/foundation")
	runCount = flag.Int("run_count", 1000, "Maximum number of runs to consider per repo.")
	maxJobs  = flag.Int("max_jobs", 1000, "Max jobs per run.")
)

func main() {
	flag.Parse()

	if err := (func(ctx context.Context) error {
		ghToken := os.Getenv("GITHUB_TOKEN")
		if ghToken == "" {
			return errors.New("GITHUB_TOKEN is required")
		}

		if *repos == "" {
			return errors.New("-repos is required")
		}

		client := github.NewClient(nil).WithAuthToken(ghToken)

		var ws []*github.WorkflowRun
		for _, reponame := range strings.Split(*repos, ",") {
			parts := strings.Split(reponame, "/")
			if len(parts) != 2 {
				return fmt.Errorf("bad repository format: %q", reponame)
			}

			for k := 1; ; k++ {
				runs, r, err := client.Actions.ListRepositoryWorkflowRuns(ctx, parts[0], parts[1], &github.ListWorkflowRunsOptions{
					ListOptions: github.ListOptions{
						PerPage: min(100, *runCount),
						Page:    k,
					},
				})
				if err != nil {
					return err
				}

				if len(runs.WorkflowRuns) == 0 {
					break
				}

				ws = append(ws, runs.WorkflowRuns...)
				log.Printf("%s: got %d runs (total: %d rate_limit: %d/%d from: %v to %v)",
					reponame, len(runs.WorkflowRuns), len(ws), r.Rate.Remaining, r.Rate.Limit,
					ws[0].CreatedAt.Time.Format(time.RFC3339), ws[len(ws)-1].CreatedAt.Time.Format(time.RFC3339),
				)
				if len(ws) >= *runCount {
					break
				}
			}
		}

		var totalminutes int64
		var regions []Region // Sorted
		var maxconcurrency int

		checkMaxConc := func(val int) {
			if val > maxconcurrency {
				maxconcurrency = val
				log.Printf("new max concurrency: %d", maxconcurrency)
			}
		}

		for _, w := range ws {
			var jobs []*github.WorkflowJob
			for k := 1; len(jobs) < *maxJobs; k++ {
				j, r, err := client.Actions.ListWorkflowJobs(ctx, *w.Repository.Owner.Login, *w.Repository.Name, *w.ID, &github.ListWorkflowJobsOptions{
					ListOptions: github.ListOptions{
						Page:    k,
						PerPage: 100,
					},
				})
				if err != nil {
					return err
				}

				if len(j.Jobs) == 0 {
					break
				}

				repo := fmt.Sprintf("%s/%s", *w.Repository.Owner.Login, *w.Repository.Name)

				for _, job := range j.Jobs {
					if job.CompletedAt == nil || job.StartedAt == nil {
						log.Printf("%d: skipped job %d: started_at=%v completed_at=%v", *w.ID, *job.ID, job.StartedAt, job.CompletedAt)
						continue
					}

					totalminutes += int64(math.Ceil(job.CompletedAt.Time.Sub(job.StartedAt.Time).Seconds() / 60))

					jobregion := Region{
						Start: job.StartedAt.UnixMilli(),
						End:   job.CompletedAt.UnixMilli(),
						JobIDs: []JobID{
							{Repository: repo, WorkflowRunID: *w.ID, JobID: *job.ID},
						},
					}

					inserted := false
					for k, reg := range regions {
						if jobregion.Start < reg.End {
							newRegions := regions[:k]
							if jobregion.Start < reg.Start {
								newRegions = append(newRegions, Region{
									Start:  jobregion.Start,
									End:    jobregion.End,
									JobIDs: jobregion.JobIDs,
								})
								newRegions = append(newRegions, regions[k:]...)
							} else {
								newRegions = append(newRegions, Region{
									Start:  reg.Start,
									End:    jobregion.Start,
									JobIDs: reg.JobIDs,
								})
								newRegions = append(newRegions, Region{
									Start:  jobregion.Start,
									End:    jobregion.End,
									JobIDs: append(jobregion.JobIDs, reg.JobIDs...),
								})
								checkMaxConc(len(reg.JobIDs) + len(jobregion.JobIDs))
								newRegions = append(newRegions, regions[k+1:]...)
							}

							regions = newRegions
							inserted = true
							break
						}
					}

					if !inserted {
						regions = append(regions, jobregion)
						checkMaxConc(len(jobregion.JobIDs))
					}
				}

				log.Printf("%s/%s: %d: got %d jobs (total_minutes: %d max_concurrency: %d%s region_count: %d rate_limit: %d/%d)",
					*w.Repository.Owner.Login, *w.Repository.Name, *w.ID, len(j.Jobs), totalminutes,
					maxconcurrency, regionRange(regions), len(regions), r.Rate.Remaining, r.Rate.Limit)

				jobs = append(jobs, j.Jobs...)
				if j.TotalCount != nil && len(jobs) == *j.TotalCount {
					break
				}
			}
		}

		f, err := os.CreateTemp("", "regionoutput.json")
		if err != nil {
			return err
		}

		defer f.Close()

		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(regions); err != nil {
			return err
		}

		log.Printf("Computed region data: %s", f.Name())

		return nil
	})(context.Background()); err != nil {
		log.Fatal(err)
	}
}

type JobID struct {
	Repository    string `json:"repo"`
	WorkflowRunID int64  `json:"workflow_run_id"`
	JobID         int64  `json:"job_id"`
}

type Region struct {
	Start  int64   `json:"start"` // Unix milliseconds
	End    int64   `json:"end"`   // Unix milliseconds
	JobIDs []JobID `json:"count"`
}

func regionRange(regions []Region) string {
	if len(regions) == 0 {
		return ""
	}

	return fmt.Sprintf(" range_start: %s range_end: %s",
		time.UnixMilli(regions[0].Start).Format(time.RFC3339),
		time.UnixMilli(regions[len(regions)-1].End).Format(time.RFC3339),
	)
}
