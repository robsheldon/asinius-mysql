<?php

/*******************************************************************************
*                                                                              *
*   Asinius\MySQL\URL                                                          *
*                                                                              *
*   Coordinates operations for MySQL URLs.                                     *
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
*   \Asinius\MySQL\URL                                                         *
*                                                                              *
*******************************************************************************/

class URL
{

    protected static $_connection = null;

    /**
     * Accept a URL object or string, and return a MySQL Connection object for
     * that URL.
     * 
     * @param   mixed       $url
     *
     * @throws  \RuntimeException
     * 
     * @return  \Asinius\MySQL\Connection
     */
    public static function open ($url)
    {
        if ( is_string($url) ) {
            $url = new \Asinius\URL($url);
        }
        if ( ! is_a($url, '\Asinius\URL') ) {
            throw new \RuntimeException("Can't open this kind of url: $url");
        }
        foreach (['username', 'password'] as $required) {
            if ( empty($url->$required) ) {
                throw new \RuntimeException("MySQL URLs need to include a $required. Example: mysql://username:password@host/dbname");
            }
        }
        if ( empty($url->path) ) {
            throw new \RuntimeException("MySQL URLs need to include the name of the database to open as the path. Example: mysql://username:password@host/dbname");
        }
        if ( empty($url->hostname) ) {
            $url->hostname = 'localhost';
        }
        $dbpath = explode('/', trim($url->path, '/'));
        $dbname = array_shift($dbpath);
        $dsn = sprintf('mysql:dbname=%s;host=%s', $dbname, $url->hostname);
        if ( ! empty($url->port) ) {
            $dsn .= ';port=' . $url->port;
        }
        $mysql = new Connection($dsn, $url->username, $url->password);
        //  This function gets called by \Asinius\URL::open(), so open the
        //  Datastream here before returning it.
        $mysql->open();
        //  If a table name was specified, start using it now.
        if ( count($dbpath) == 1 ) {
            $mysql->use_table(array_shift($dbpath));
        }
        return $mysql;
    }


}
