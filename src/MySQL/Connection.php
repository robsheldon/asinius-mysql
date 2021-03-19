<?php

/*******************************************************************************
*                                                                              *
*   Asinius\MySQL\Connection                                                   *
*                                                                              *
*   Component that provides PDO-based MySQL access in a Datastream context.    *
*                                                                              *
*   This component doesn't provide a class specifically for database result    *
*   sets because that tends to lead to code that builds a query across a       *
*   dozen classes, functions, and files, and then processes the results        *
*   across another dozen classes, functions, and files. That kind of code is   *
*   hard to follow and debug. The approach here occasionally feels             *
*   constrained but it also tends to keep database-related code clumped        *
*   together.                                                                  *
*                                                                              *
*   These notes are here to talk me out of changing this structure later.      *
*                                                                              *
*   LICENSE                                                                    *
*                                                                              *
*   Copyright (c) 2020 Rob Sheldon <rob@rescue.dev>                            *
*                                                                              *
*   Permission is hereby granted, free of charge, to any person obtaining a    *
*   copy of this software and associated documentation files (the "Software"), *
*   to deal in the Software without restriction, including without limitation  *
*   the rights to use, copy, modify, merge, publish, distribute, sublicense,   *
*   and/or sell copies of the Software, and to permit persons to whom the      *
*   Software is furnished to do so, subject to the following conditions:       *
*                                                                              *
*   The above copyright notice and this permission notice shall be included    *
*   in all copies or substantial portions of the Software.                     *
*                                                                              *
*   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS    *
*   OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF                 *
*   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.     *
*   IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY       *
*   CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,       *
*   TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE          *
*   SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.                     *
*                                                                              *
*   https://opensource.org/licenses/MIT                                        *
*                                                                              *
*******************************************************************************/

namespace Asinius\MySQL;


/*******************************************************************************
*                                                                              *
*   \Asinius\MySQL\Connection                                                  *
*                                                                              *
*******************************************************************************/

class Connection implements \Asinius\Datastream
{

    protected $_properties      = [];
    protected $_pdo             = null;
    protected $_pdo_arguments   = [];
    protected $_pdo_statement   = null;
    protected $_pdo_results     = [];
    protected $_pdo_result_idx  = 0;
    protected $_log             = [];
    protected $_log_interfaces  = [];
    protected $_state           = \Asinius\Datastream::STREAM_UNOPENED;
    protected $_last_statement  = '';
    protected $_last_arguments  = [];
    protected $_default_table   = '';
    protected $_table_columns   = [];


    /**
     * Return a new MySQL Connection object.
     */
    public function __construct ($dsn)
    {
        $arguments = func_get_args();
        if ( count($arguments) > 4 ) {
            throw new \RuntimeException("Bad argument count for \" new" . __CLASS__ . "()'\"");
        }
        $this->_pdo_arguments = $arguments;
        //  Connection gets opened explicitly in open() below.
    }


    /**
     * Shut down the current MySQL Connection.
     */
    public function __destruct ()
    {
        //  I dunno. Required by all Datastream interfaces.
        $this->close();
    }


    /**
     * Special properties currently supported:
     * 
     * throw_on_error: Set to false to prevent exceptions on database query errors.
     * log_limit: Limit log to this many statements.
     * 
     * @param   string      $property
     */
    public function __get ($property)
    {
        if ( ! array_key_exists($property, $this->_properties) ) {
            throw new \RuntimeException("Undefined property: \"$property\"");
        }
        return $this->_properties[$property];
    }


    /**
     * Set a property and value.
     * 
     * @param   string      $property
     * @param   mixed       $value
     */
    public function __set ($property, $value)
    {
        $this->_properties[$property] = $value;
    }


    /**
     * Return true if a property is set.
     * 
     * @param   string      $property
     * 
     * @return  boolean
     */
    public function __isset ($property)
    {
        return array_key_exists($property, $this->_properties);
    }


    /**
     * Support for cloning MySQL Connections.
     * 
     * @return  void
     */
    public function __clone ()
    {
        //  Reset everything except for file and db references.
        $this->_log = [];
        $this->_last_query_result = false;
        $this->reset();
    }


    /**
     * Write a message to the internal log, and optionally to any currently
     * defined log interfaces (files, etc.)
     * 
     * @param   string      $line
     * @param   mixed       $destination
     *
     * @internal
     * 
     * @return  void
     */
    protected function _log ($line, $destination = null)
    {
        if ( is_null($destination) ) {
            $this->_log[] = $line;
            if ( array_key_exists('log_limit', $this->_properties) && is_int($this->_properties['log_limit']) ) {
                if ( $this->_properties['log_limit'] < 1 ) {
                    $this->_log = [];
                }
                else {
                    array_splice($this->_log, 0, count($this->_log) - $this->_properties['log_limit']);
                }
            }
            $destinations = $this->_log_interfaces;
        }
        else {
            $destinations = [$destination];
        }
        foreach ($destinations as $out) {
            if ( is_string($out) ) {
                file_put_contents($out, $line . "\n", FILE_APPEND);
            }
            else if ( is_resource($out) ) {
                fwrite($out, $line . "\n");
            }
            else if ( is_object($out) && is_a($out, '\Asinius\Datastream') ) {
                $out->write($line . "\n");
            }
        }
    }


    /**
     * Save a SQL statement and its parameters to the internal log.
     *
     * @param   mixed       $message
     *
     * @return  void
     */
    protected function _log_statement ($statement, $parameters)
    {
        $this->_log(implode(' ', [date('c'), 'SQL:', \Asinius\Functions::escape_str($statement), '<<', \Asinius\Functions::to_str($parameters)]));
    }


    /**
     * Parse the arguments passed to a query function into the statement and its
     * parameters, if any.
     *
     * @param   array       $arguments
     *
     * @return  array
     */
    protected function _parse_statement_arguments ($arguments)
    {
        $statement = array_shift($arguments);
        //  Allow statement parameters to be passed as an array in a single argument.
        if ( count($arguments) == 1 && is_array($arguments[0]) ) {
            $arguments = $arguments[0];
        }
        //  Expand nested array arguments (for those 'WHERE...IN...' statements).
        $expanded = [];
        $n = count($arguments);
        while ( $n-- ) {
            if ( is_array($arguments[$n]) ) {
                $expanded = array_merge($arguments[$n], $expanded);
            }
            else {
                array_unshift($expanded, $arguments[$n]);
            }
        }
        return [$statement, $expanded];
    }


    /**
     * Attempt to detect if a query returned sucessfully. Returns true if there's
     * any uncertainty.
     *
     * @param   PDOStatement    $query
     *
     * @return  boolean
     */
    protected function _check_result ($query)
    {
        //  Some statements (insert, update, delete) trigger an error code that isn't actually
        //  an error code. This function handles that for everything else.
        if ( is_object($query) && is_a($query, 'PDOStatement') ) {
            $error_info = $query->errorInfo();
            if ( $error_info[0] != '00000' && ! ($error_info[0] == 'HY000' && empty($error_info[1])) ) {
                $error_string = 'MySQL ERROR ' . $error_info[0] . ' (' . $error_info[1] . '): ' . $error_info[2];
                if ( ! array_key_exists('throw_on_error', $this->_properties) || $this->_properties['throw_on_error'] !== false ) {
                    /* 
                        By default, if a query fails with an error from the database
                        engine, it will throw() here. You can disable that behavior
                        by setting "->throw_on_error = false", but beware, doing
                        so will eventually cause much wailing and gnashing of teeth.
                    */
                    throw new \RuntimeException($error_string . "\nLast statement was: " . end($this->_log));
                }
                $this->_log($error_string);
                return false;
            }
        }
        return true;
    }


    /**
     * Add a destination for the internal log: the path to a file, or a file
     * resource, or a Datastream.
     *
     * @param   mixed       $destination
     *
     * @return  void
     */
    public function log_to ($destination)
    {
        $this->_log_interfaces[] = $destination;
        //  Immediately send any stored messages to this destination.
        foreach ($this->_log as $line) {
            $this->_log($line, $destination);
        }
    }


    /**
     * Write a message from the application to the internal log.
     *
     * @param   string      $message
     *
     * @return  void
     */
    public function log_msg ($message)
    {
        $this->_log('APPMSG: ' . $message);
    }
    

    /**
     * Open a connection to the MySQL database.
     * 
     * @return  void
     */
    public function open ()
    {
        $this->_pdo = new \PDO(...$this->_pdo_arguments);
        $this->_state = \Asinius\Datastream::STREAM_CONNECTED;
    }


    /**
     * Returns true if the specified table exists in this database, false otherwise.
     *
     * @param   string      $table_name
     *
     * @throws  \RuntimeException
     *
     * @return  boolean
     */
    public function table_exists ($table_name)
    {
        if ( ! is_string($table_name) ) {
            throw new \RuntimeException("\$table_name must be a string type");
        }
        $this->ready(true);
        //  This bypasses all of the nice work done in search() so that a call
        //  to this function during a search()/read() cycle won't interrupt
        //  those results.
        $pdo_statement  = $this->_pdo->prepare('SHOW TABLES LIKE ?');
        $pdo_statement->execute([$table_name]);
        $tables = $pdo_statement->fetch(\PDO::FETCH_ASSOC);
        return $tables !== false;
    }


    /**
     * Use a specific table by default. Allows for passing simple arrays to
     * write(), as well as read()ing from the table without a prior search().
     *
     * @param   string      $table_name
     *
     * @throws  \RuntimeException
     *
     * @return  boolean
     */
    public function use_table ($table_name)
    {
        if ( ! is_string($table_name) ) {
            throw new \RuntimeException("\$table_name must be a string type");
        }
        if ( strlen($table_name) == 0 ) {
            //  Unset the current table.
            $this->_default_table = '';
            $this->_table_columns = [];
            return true;
        }
        //  A parameterized query can't be used here so the table name needs to
        //  be validated. This library enforces a stricter character set for
        //  table names than MySQL does by default.
        if ( strspn($table_name, 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_') !== strlen($table_name) ) {
            throw new \RuntimeException("Invalid table name: $table_name. Table names can only contain [A-Za-z0-9_]");
        }
        $this->ready(true);
        if ( ! $this->table_exists($table_name) ) {
            throw new \RuntimeException("Table does not exist in database: $table_name");
        }
        //  Load the table definition so that array keys can be associated with
        //  table columns during write() operations. Like table_exists() above,
        //  this bypasses the usual search()/read() code path so that in-progress
        //  operations don't get interrupted.
        //  Unfortunately parameterized queries don't work here, but since the
        //  table was verified in table_exists(), it's probably -- usually --
        //  okay to proceed here.
        $pdo_statement = $this->_pdo->prepare("DESCRIBE `$table_name`");
        $pdo_statement->execute([]);
        $columns = [];
        while ( ($column = $pdo_statement->fetch(\PDO::FETCH_ASSOC)) !== false ) {
            $columns[$column['Field']] = ['type' => $column['Type'], 'null' => $column['Null'], 'default' => $column['Default'], 'extra' => $column['Extra']];
        }
        $this->_table_columns = $columns;
        $this->_default_table = $table_name;
        return true;
    }


    /**
     * Return true if the object is not in an error condition, false otherwise.
     * If $throw is set, then this function becomes an assertion, and will
     * throw() an exception describing the current connection state. This is
     * mostly for internal use, but feel free to use it too.
     *
     * @param   boolean     $throw
     *
     * @throws  \RuntimeException
     *
     * @return  boolean
     */
    public function ready ($throw = false)
    {
        $ready = ! is_null($this->_pdo) && $this->_state === \Asinius\Datastream::STREAM_CONNECTED;
        if ( ! $throw ) {
            return $ready;
        }
        if ( ! $ready ) {
            if ( is_null($this->_pdo) ) {
                throw new \RuntimeException('Not connected to database');
            }
            if ( $this->_state !== \Asinius\Datastream::STREAM_CONNECTED ) {
                throw new \RuntimeException('Database connection not ready: closed or in error');
            }
            throw new \RuntimeException('Database connection is not ready');
        }
        return true;
    }


    /**
     * Return any errors stored in the log.
     *
     * @return  array
     */
    public function errors ()
    {
        return preg_grep('/^MySQL ERROR/', $this->_log);
    }


    /**
     * Return any messages stored in the log.
     *
     * @return  array
     */
    public function log ()
    {
        return $this->_log;
    }


    /**
     * Reset the current query state.
     *
     * @return  void
     */
    public function reset ()
    {
        //  Reset last query cache; some applications may need to perform the same query
        //  more than once and that could cause them to receive an incorrect result.
        $this->_last_statement = '';
        $this->_last_arguments = [];
        $this->_pdo_results    = [];
        $this->_pdo_result_idx = 0;
    }


    /**
     * Query the connected database with a statement and (optionally) parameters, and
     * get either an associative array back with results (for e.g. "SELECT..."), or
     * false (if the query failed), or true (for e.g. a successful "INSERT...").
     *
     * @return  void
     */
    public function search ($query_string)
    {
        //  Accepts variable arguments; first arg is always the query string, rest are values to use.
        $arguments = func_get_args();
        if ( count($arguments) == 0 ) {
            throw new \RuntimeException('No arguments?');
        }
        list($statement, $arguments) = $this->_parse_statement_arguments($arguments);
        if ( $this->_last_statement == $statement && $this->_last_arguments === $arguments ) {
            return;
        }
        //  New query.
        $this->_log_statement($statement, $arguments);
        $this->ready(true);
        $this->_last_statement = $statement;
        $this->_last_arguments = $arguments;
        $this->_pdo_results    = [];
        $this->_pdo_result_idx = 0;
        $this->_pdo_statement  = $this->_pdo->prepare($statement);
        $this->_pdo_statement->execute($arguments);
        $this->_check_result($this->_pdo_statement);
        //  Preload the first row of the result so that questions like
        //  empty() and peek() can be answered.
        $this->_pdo_results[] = $this->_pdo_statement->fetch(\PDO::FETCH_ASSOC);
    }


    /**
     * Returns true if there are no more rows in the result, false otherwise.
     * 
     * @return  boolean
     */
    public function empty ()
    {
        return $this->_pdo_results[$this->_pdo_result_idx] == false;
    }


    /**
     * Return the next row in the result set or false if there are no more rows.
     * If a default table has been selected with use_table(), then the application
     * can read() all rows from the table without calling search() first.
     * WARNING: Doing this could be slooooooooooow.
     *
     * @return  mixed
     */
    public function read ()
    {
        if ( empty($this->_last_statement) && $this->_default_table != '' ) {
            $table = $this->_default_table;
            $this->search("SELECT * FROM `$table`");
        }
        $value = $this->_pdo_results[$this->_pdo_result_idx];
        if ( $value !== false ) {
            $this->_pdo_results[] = $this->_pdo_statement->fetch(\PDO::FETCH_ASSOC);
            $this->_pdo_result_idx++;
        }
        return $value;
    }


    /**
     * Return the next row in the result set, but do not load the next row. If
     * read() is called after peek(), it will return the same row as peek().
     *
     * @return  mixed
     */
    public function peek ()
    {
        return $this->_pdo_results[$this->_pdo_result_idx];
    }


    /**
     * Rewind some number of rows and return that row, or false if there aren't
     * that many rows to rewind.
     *
     * @return  mixed
     */
    public function rewind ($rows = 1)
    {
        if ( $rows < 1 ) {
            throw new \RuntimeException("Can't rewind $rows rows");
        }
        if ( $rows > $this->_pdo_result_idx ) {
            $this->_pdo_result_idx = 0;
            return false;
        }
        $this->_pdo_result_idx -= $rows;
        return $this->_pdo_results[$this->_pdo_result_idx];
    }


    /**
     * "Write" a statement to the database: essentially this means executing
     * some statement without storing the results internally. This is best
     * used for insert, update, delete, etc. statements and will not disrupt
     * any select statement already in progress.
     *
     * @param   mixed       $statement
     *
     * @throws  \RuntimeException
     *
     * @return  mixed
     */
    public function write ($statement)
    {
        if ( is_array($statement) && $this->_default_table != '' ) {
            //  Allow applications to write key-value arrays directly to database
            //  tables after calling use_table().
            //  Make sure all required columns are included and then create a
            //  statement to execute.
            //  I don't want to allow positional array keys because I think
            //  that's just asking for trouble later. Applications that insist
            //  on using positional data can just call array_combine() before
            //  calling write().
            foreach ($this->_table_columns as $name => $properties) {
                if ( strtolower($properties['null']) == 'no' && is_null($properties['default']) && ! array_key_exists($name, $statement) && ! $properties['extra'] == 'auto_increment' ) {
                    throw new \RuntimeException("Missing required column during write(): $name");
                }
            }
            //  Silently ignore any keys in the data that don't match a column
            //  in the database. This allows applications to dump a single data
            //  structure into multiple destinations, including this table.
            $write_columns = array_intersect_key($statement, $this->_table_columns);
            //  Now build the insert/update statement.
            $table = $this->_default_table;
            $column_names = '`' . implode('`, `', array_keys($write_columns)) . '`';
            $column_placeholders = implode(', ', array_fill(0, count($write_columns), '?'));
            $statement = "
                INSERT `$table` ($column_names)
                VALUES ($column_placeholders)
                ON DUPLICATE KEY UPDATE ";
            $statement .= implode(', ', array_map(function($column){
                return "`$column` = VALUES(`$column`)";
            }, array_keys($write_columns)));
            $arguments = array_values($write_columns);
        }
        else {
            //  Accept variable arguments; first arg is always the query string, rest are values to use.
            $arguments = func_get_args();
            if ( count($arguments) == 0 ) {
                throw new \RuntimeException('No arguments?');
            }
            list($statement, $arguments) = $this->_parse_statement_arguments($arguments);
        }
        $this->_log_statement($statement, $arguments);
        $this->ready(true);
        $pdo_statement = $this->_pdo->prepare($statement);
        $pdo_statement->execute($arguments);
        $this->_check_result($this->_pdo_statement);
        //  fetch() or fetchAll() can't be called on update(), insert(), etc.
        //  statements, because even if they succeed, they still return an
        //  error code -- a "general error" code which can't be filtered out
        //  as an "everything's OK but don't call fetch() on insert()"
        //  error condition.
        $command = strtolower(substr($statement, 0, 6));
        if ( $command != 'insert' && $command != 'update' && $command != 'delete' ) {
            return $pdo_statement->fetch(\PDO::FETCH_ASSOC);
        }
        return true;
    }


    /**
     * Close the Datastream / MySQL connection. If open() is called after close(),
     * the connection will be reopened with the same connection information
     * originally provided to open().
     *
     * @return  void
     */
    public function close ()
    {
        $this->_pdo             = null;
        $this->_pdo_statement   = null;
        $this->_state           = \Asinius\Datastream::STREAM_CLOSED;
        $this->reset();
    }

}
