// Renders gang-*.slurm with the slurm plugin's own gang generator. The go tool
// ignores testdata: copy this file into the plugin's pkg/slurm and run
//
//	FIXTURE_OUT=<this directory> go test ./pkg/slurm -run TestRenderBridgeFixtures -count=1
package slurm

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRenderBridgeFixtures(t *testing.T) {
	out := os.Getenv("FIXTURE_OUT")
	if out == "" {
		t.Skip("FIXTURE_OUT not set")
	}
	// The generator creates the job directories, so it renders below a
	// temporary root that is then replaced by a fixed one.
	const root = "/scratch/u/interlink/jobs/"
	head, worker := "0b6f3c1e-8f0a-4d7e-9c2b-5a1d3e4f6a70", "7c9e2a4b-1d3f-4e5a-8b6c-9d0e1f2a3b4c"
	cases := map[string]map[string]string{
		"gang-per-rank.slurm": nil,
		"gang-launcher.slurm": {LauncherAnnotation: "torchrun", testFlagsAnnotation: testGres4},
	}
	for name, extra := range cases {
		dataRoot := t.TempDir() + string(os.PathSeparator)
		config := SlurmConfig{BashPath: testBashPath, Commandprefix: testModuleLoad, DataRootFolder: dataRoot}
		if extra != nil {
			config.SiteScriptPath = writeSite(t, testSite)
		}
		entry := &GangEntry{Name: "g1", Size: 2, Members: map[string]*BufferedMember{
			head:   mkGangMemberWithRC(t, dataRoot, head, GangRoleHead, 2, extra, []string{testPython}, []string{testTrainScript}),
			worker: mkGangMemberWithRC(t, dataRoot, worker, GangRoleWorker, 2, extra, []string{testPython}, []string{testTrainScript}),
		}}
		path, err := produceGangSLURMScript(context.Background(), config, entry, assignRanks(entry.Members))
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		s := strings.ReplaceAll(string(b), dataRoot, root)
		if config.SiteScriptPath != "" {
			s = strings.ReplaceAll(s, config.SiteScriptPath, "/etc/interlink/site.sh")
		}
		if err := os.WriteFile(filepath.Join(out, name), []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
