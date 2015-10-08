package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/libgit2/git2go"
	_ "github.com/minus5/gofreetds"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

// config holds database, output location & git config
type config struct {
	db dbConfig
	// Full path to the directory where SPs should be saved
	outDir string
	// User name to use when committing
	gitName string
	// Email address to use when committing
	gitEmail string
}

// isValid checks if the config is valid.
func (c *config) isValid() error {
	if len(c.outDir) == 0 {
		return errors.New("Missing output directory")
	}

	if _, err := os.Stat(c.outDir); os.IsNotExist(err) {
		return errors.New("Output directory does not exist")
	}

	if len(c.gitName) == 0 {
		return errors.New("No git username provided")
	}

	if len(c.gitEmail) == 0 {
		return errors.New("No git email provided")
	}

	return c.db.isValid()
}

// dbConfig holds config that is specific to the database server.
type dbConfig struct {
	host     string
	database string
	user     string
	password string
}

// isValid checks if the config is valid.
func (c *dbConfig) isValid() error {
	if len(c.host) == 0 {
		return errors.New("Missing host")
	}

	if len(c.database) == 0 {
		return errors.New("Missing database name")
	}

	return nil
}

// connectionString converts the config to a connection string.
func (c *dbConfig) connectionString() string {
	return fmt.Sprintf("host=%v;database=%v;user=%v;pwd=%v", c.host, c.database, c.user, c.password)
}

// Global config variable
var conf = config{}

func init() {
	flag.StringVar(&conf.db.host, "h", "127.0.0.1", "database host")
	flag.StringVar(&conf.db.database, "d", "", "database name")
	flag.StringVar(&conf.db.user, "u", "sa", "database username")
	flag.StringVar(&conf.db.password, "p", "", "database password")
	flag.StringVar(&conf.outDir, "o", "", "output directory")
	flag.StringVar(&conf.gitName, "n", "spkeeper", "git commit name")
	flag.StringVar(&conf.gitEmail, "e", "spkeeper@example.com", "git commit email")
}

// checkFatal will exit with an error status when given a non-nil error
func checkFatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

// getProcedureNames gets all stored procedure names from the given database
func getProcedureNames(db *sqlx.DB) (names []string, err error) {
	err = db.Select(&names, "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE='PROCEDURE'")

	return names, err
}

// saveProcedure saves the stored procedure with the given name to disk.
// The SP body will be written to a file named after the SP in outDir.
func saveProcedure(db *sqlx.DB, name string, outDir string) (err error) {
	outFileName := filepath.Join(conf.outDir, conf.db.database, fmt.Sprintf("%v.sql", name))
	f, err := os.Create(outFileName)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Printf("Writing %v to: %v\n", name, outFileName)

	w := bufio.NewWriter(f)
	defer w.Flush()

	return writeProcedureBody(db, name, w)
}

// writeProcedureBody reads the body of the SP with the given name from the database and writes it
// to the given Writer.
func writeProcedureBody(db *sqlx.DB, name string, w io.Writer) (err error) {
	rows, err := db.Query("EXEC sp_helptext ?", name)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var text string
		err = rows.Scan(&text)
		if err != nil {
			return errors.New(fmt.Sprintf("%v: %v", name, err.Error()))
		}

		_, err = w.Write([]byte(text))
		if err != nil {
			return errors.New(fmt.Sprintf("%v: %v", name, err.Error()))
		}
	}
	return nil
}

// spSaver receives a stored procedure name on the names chan, saves the body of the named procedure
// to disk and returns any error on the results chan.
func spSaver(db *sqlx.DB, outDir string, names <-chan string, results chan<- error) {
	for n := range names {
		results <- saveProcedure(db, n, outDir)
	}
}

// saveAllProcedures saves all of the stored procedures with the names given to a subdirectory of
// the outDir specified in the passed config. This sub directory will be named after the database
// they are being read from.
// workerCount goroutines will be used for fetching procure bodies from the database.
func saveAllProcedures(names []string, workerCount int, db *sqlx.DB, conf config) (err error) {
	// Ensure the output subdirectory exists
	// Make the db subdirectory
	outDir := filepath.Join(conf.outDir, conf.db.database)
	err = os.MkdirAll(outDir, 0700)
	if err != nil {
		return err
	}

	saveNames := make(chan string, 100)
	results := make(chan error, 100)
	done := make(chan bool)
	var errCount uint64 = 0

	fmt.Println("Saving", len(names), "stored procedures")

	// Print any errors that occur when saving
	go func() {
		for i := 0; i < len(names); i++ {
			err = <-results
			if err != nil {
				// Report but don't fail on an SP's body being unreadable
				if strings.Contains(err.Error(), "sql: expected 2 destination arguments in Scan, not 1") {
					fmt.Fprintf(os.Stderr, "Error reading SQL for %v\n", err.Error())
				} else {
					fmt.Fprintln(os.Stderr, err.Error())
					atomic.AddUint64(&errCount, 1)
				}
			}
		}

		done <- true
	}()

	// Make our workers that will do the saving
	for w := 0; w <= workerCount; w++ {
		go spSaver(db, outDir, saveNames, results)
	}

	// Send all our SP names to our workers
	for _, name := range names {
		saveNames <- name
	}
	close(saveNames)

	<-done

	finalErrCount := atomic.LoadUint64(&errCount)
	if finalErrCount > 0 {
		return errors.New(fmt.Sprintf("%v errors occured while saving stored procedures", finalErrCount))
	}

	return nil
}

// getRepo either gets the existing repo from the given path or initialise a new one.
func getRepo(repoPath string) (repo *git.Repository, err error) {
	// Return the repo if we have one already
	repo, err = git.OpenRepository(repoPath)
	// Or init a new one
	if err != nil {
		repo, err = git.InitRepository(repoPath, false)
	}

	return repo, err
}

// commitChanges creates a new commit containing all changes found in the given config's outDir.
func commitChanges(repo *git.Repository, conf config) (err error) {
	// Add all SP files to the index
	idx, err := repo.Index()
	if err != nil {
		return err
	}

	changedFiles := make([]string, 0, 0)

	idx.AddAll([]string{filepath.Join(conf.db.database, "*")}, git.IndexAddDefault, func(path, spec string) int {
		changedFiles = append(changedFiles, path)
		return 0
	})
	if err != nil {
		return err
	}

	// If nothing has changed in the index, we can finish here
	if len(changedFiles) == 0 {
		fmt.Println("No changes to commit")
		return nil
	}

	treeId, err := idx.WriteTree()
	if err != nil {
		return err
	}

	err = idx.Write()
	if err != nil {
		return err
	}

	// Get stuff we need to create a commit
	tree, err := repo.LookupTree(treeId)
	if err != nil {
		return err
	}

	headCommit, err := getHeadCommit(repo)
	if err != nil {
		return err
	}

	signature := &git.Signature{
		Name:  conf.gitName,
		Email: conf.gitEmail,
		When:  time.Now(),
	}

	message := buildCommitMessage(conf.db.database, changedFiles)

	if headCommit != nil {
		fmt.Printf("Committing updates to %v files\n", len(changedFiles))
		_, err = repo.CreateCommit("refs/heads/master", signature, signature, message, tree, headCommit)
	} else {
		fmt.Printf("Creating initial commit containing %v files\n", len(changedFiles))
		_, err = repo.CreateCommit("refs/heads/master", signature, signature, message, tree)
	}

	return err
}

// getHeadCommit gets the head commit from master for the given repo, or nil if the repo is empty
func getHeadCommit(repo *git.Repository) (commit *git.Commit, err error) {
	// Check if this is a new repo
	_, err = repo.Head()
	if err != nil && git.IsErrorCode(err, git.ErrUnbornBranch) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	master, err := repo.LookupBranch("master", git.BranchLocal)
	if err != nil {
		return nil, err
	}

	commit, err = repo.LookupCommit(master.Target())

	return commit, err
}

// buildCommitMessage builds a commit message detailing which database the commit concerns and which
// files have been changed
func buildCommitMessage(database string, changedPaths []string) string {
	pathsString := strings.Join(changedPaths, "\n")
	return fmt.Sprintf("Update with procedures from database '%v'\n\nThese files have changed:\n\n%v", database, pathsString)
}

func main() {
	flag.Parse()
	checkFatal(conf.isValid())

	db, err := sqlx.Connect("mssql", conf.db.connectionString())
	checkFatal(err)

	// Get all of the SP names from the database
	names, err := getProcedureNames(db)
	checkFatal(err)

	checkFatal(saveAllProcedures(names, 5, db, conf))

	// Get or init a git repo
	repo, err := getRepo(conf.outDir)
	checkFatal(err)

	// Commit all changed files
	checkFatal(commitChanges(repo, conf))
}
