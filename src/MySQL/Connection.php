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
    protected $_state           = \Asinius\Datastream::STATUS_CLOSED;
    protected $_last_statement  = '';
    protected $_last_arguments  = [];


    /**
     * Return a new MySQL Connection object.
     */
    public function __construct ()
    {
        $arguments = func_get_args();
        if ( count($arguments) < 1 || count($arguments) > 4 ) {
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
     * The only special property currently supported is throw_on_error. If it's
     * set to false, database query errors will not throw an exception.
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
        $this->_log[] = implode(' ', [date('c'), 'SQL:', \Asinius\Functions::escape_str($statement), '<<', \Asinius\Functions::to_str($parameters)]);
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
        if ( count($arguments) == 0 ) {
            return [];
        }
        $statement = array_shift($arguments);
        //  Allow statement parameters to be passed as an array in a single argument.
        if ( count($arguments) == 1 && is_array($arguments[0]) ) {
            return [$statement => $arguments[0]];
        }
        return [$statement => $arguments];
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
        $this->_state = \Asinius\Datastream::STATUS_READY;
    }


    /**
     * Return true if the object is not in an error condition, false otherwise.
     *
     * @return  boolean
     */
    public function ready ()
    {
        return ! is_null($this->_pdo) && $this->_state === \Asinius\Datastream::STATUS_READY;
    }


    /**
     * Return any messages stored in the log.
     *
     * @return  array
     */
    public function errors ()
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
    public function search ()
    {
        //  Accepts variable arguments; first arg is always the query string, rest are values to use.
        $statement_and_args = $this->_parse_statement_arguments(func_get_args());
        foreach ($statement_and_args as $statement => $arguments) {
            if ( $this->_last_statement == $statement && $this->_last_arguments == $arguments ) {
                continue;
            }
            //  New query.
            $this->_log_statement($statement, $arguments);
            if ( ! $this->ready() ) {
                if ( is_null($this->_pdo) ) {
                    throw new \RuntimeException("Not connected to database");
                }
                if ( $this->_state !== \Asinius\Datastream::STATUS_READY ) {
                    throw new \RuntimeException("Database connection not ready: closed or in error");
                }
                throw new \RuntimeException("Database connection is not ready");
            }
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
            return;
        }
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
     *
     * @return  mixed
     */
    public function read ()
    {
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
     * @return  mixed
     */
    public function write ()
    {
        //  Accepts variable arguments; first arg is always the query string, rest are values to use.
        $statement_and_args = $this->_parse_statement_arguments(func_get_args());
        foreach ($statement_and_args as $statement => $arguments) {
            $this->_log_statement($statement, $arguments);
            if ( ! $this->ready() ) {
                if ( is_null($this->_pdo) ) {
                    throw new \RuntimeException("Not connected to database");
                }
                if ( $this->_state !== \Asinius\Datastream::STATUS_READY ) {
                    throw new \RuntimeException("Database connection not ready: closed or in error");
                }
                throw new \RuntimeException("Database connection is not ready");
            }
            $pdo_statemnt = $this->_pdo->prepare($statement);
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
        $this->_state           = \Asinius\Datastream::STATUS_CLOSED;
        $this->reset();
    }

}
