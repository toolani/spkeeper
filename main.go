package main

import (
	// "database/sql"
	"bufio"
	"errors"
	"flag"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/minus5/gofreetds"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
)

type config struct {
	db     dbConfig
	outDir string
}

func (c *config) isValid() error {
	if len(c.outDir) == 0 {
		return errors.New("Missing output directory")
	}

	if _, err := os.Stat(c.outDir); os.IsNotExist(err) {
		return errors.New("Output directory does not exist")
	}

	return c.db.isValid()
}

type dbConfig struct {
	host     string
	database string
	user     string
	password string
}

func (c *dbConfig) isValid() error {
	if len(c.host) == 0 {
		return errors.New("Missing host")
	}

	if len(c.database) == 0 {
		return errors.New("Missing database name")
	}

	return nil
}

func (c *dbConfig) connectionString() string {
	return fmt.Sprintf("host=%v;database=%v;user=%v;pwd=%v", c.host, c.database, c.user, c.password)
}

var conf = config{}

func init() {
	flag.StringVar(&conf.db.host, "h", "127.0.0.1", "database host")
	flag.StringVar(&conf.db.database, "d", "", "database name")
	flag.StringVar(&conf.db.user, "u", "sa", "database username")
	flag.StringVar(&conf.db.password, "p", "", "database password")
	flag.StringVar(&conf.outDir, "o", "", "output directory")
}

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
		checkFatal(err)

		_, err = w.Write([]byte(text))
		if err != nil {
			return err
		}
	}
	return nil
}

// spSaver receives a stored procedure name on the names chan, saves the body of the named procedure
// to disk and returns any error on the results chan
func spSaver(db *sqlx.DB, outDir string, names <-chan string, results chan<- error) {
	for n := range names {
		results <- saveProcedure(db, n, outDir)
	}
}

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
				fmt.Fprintf(os.Stderr, err.Error())
				atomic.AddUint64(&errCount, 1)
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

func main() {
	flag.Parse()
	checkFatal(conf.isValid())

	db, err := sqlx.Connect("mssql", conf.db.connectionString())
	checkFatal(err)

	// Get all of the SP names from the database
	names, err := getProcedureNames(db)
	checkFatal(err)

	checkFatal(saveAllProcedures(names, 5, db, conf))
}
