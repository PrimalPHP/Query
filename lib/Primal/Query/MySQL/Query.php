<?php 
namespace Primal\Query\MySQL;
use \PDO;
use \DateTime;

/**
 * Primal\Query for MySQL - Asynchronous, chain-able SQL Query Builder
 * http://github.com/PrimalPHP/Query
 *
 * @package Primal/Query
 * @author Jarvis Badgley
 * @copyright 2008 - 2013 Jarvis Badgley
 * @license MIT
 */

class Query {
	/**
	 * Return type constants
	 */
	const RETURN_NONE = -1;
	const RETURN_FULL = 0;
	const RETURN_SINGLE_ROW = 1;
	const RETURN_SINGLE_COLUMN = 2;
	const RETURN_SINGLE_CELL = 3;


	/**
	 * Table name
	 * @var string
	 */	
	protected $table;

	/**
	 * Data values used for bound parameters
	 * @var array
	 */	
	protected $parameters = array();

	/**
	 * Individual search conditions
	 * @var array
	 */	
	protected $where = array();

	/**
	 * Individual search conditions
	 * @var array
	 */	
	protected $set = array();
	
	/**
	 * Columns to return on the search
	 * @var string
	 */	
	protected $return = array('*');

	/**
	 * Table joins, multidimensional array
	 * @var array
	 */	
	protected $joins = array();

	/**
	 * Boolean condition for individual search items
	 * @var string
	 */	
	protected $boolean = 'AND';
	
	/**
	 * ORDER BY clause
	 * @var string
	 */	
	protected $orderby;
	
	/**
	 * GROUP BY clause
	 * @var string
	 */	
	protected $groupby;
	
	/**
	 * DISTINCT condition
	 * @var boolean
	 */	
	protected $distinct=false;
	
	/**
	 * PDO object used for communication with the server
	 *
	 * @var array
	 * @access protected
	 */
	protected $pdolink;
	
	
	
	/**
	 * Static function for returning a new object, allowing function chaining without declaring a variable.
	 *
	 * @static
	 * @param string $pdo The name of the primary table to be searched against
	 * @param string $alias An alias for the table name to be used in the query
	 * @return Query A new Query object
	 */
	static function Make($pdo = null) {
		return new static($pdo);
	}
	
	/**
	 * Class Constructor
	 *
	 * @param string $target The name of the primary table to be searched against, OR the PDO link to use for making a request
	 * @param string $alias An alias for the table name to be used in the query
	 */
	function __construct($pdo = null) {
		if ($pdo !== null) {
			$this->setPDOLink($pdo);
		}
	}
	
	
	/**
	 * Defines the PDO object used for performing queries.
	 *
	 * @param PDO $pdo 
	 * @return Query The current Query object.
	 */
	function setPDOLink($pdo) {
		if ($pdo instanceof PDO) {
			$this->pdolink = $pdo;
		} else {
			throw new InvalidArgumentException("Expected PDO object for first argument.");
		}
		return $this;
	}


	/**
	 * Returns the PDO object to be used for performing queries.
	 * Throws an exception if unable to find a link.
	 *
	 * @return PDO
	 */
	function getPDOLink() {
		if (!$this->pdolink) throw new QueryException("No PDO link has been defined.");

		return $this->pdolink;
	}
	
		
	/**
	 * Defines the table name and optional alias.
	 *
	 * @param string $tablename 
	 * @param string $alias 
	 * @return Query The current Query object.
	 */
	function from($tablename, $alias='') {
		$this->table = trim("`$tablename` $alias");
		return $this;
	}
	

	/**
	 * Add an unnamed value to the paramater collection.
	 *
	 * @param mixed $data Data to be added 
	 * @return string Name generated for the value
	 **/
	function createParam($data) {
		$key = uniqid(':P');
		$this->insertParam($data, $key);
		return $key;
	}
	

	/**
	 * Add a named value to the paramater collection.
	 *
	 * @param mixed $data Data to be added 
	 * @param mixed $key Parameter name. If leading colon is omitted, it will be prepended. 
	 * @return Query The current Query object.
	 **/
	function insertParam($data, $key) {
		if ($key[0] !== ':') $key = ":$key";
		$this->parameters[$key] = $data;
		return $this;
	}

/**
	WHERE CONDITIONS
*/	

	/**
	 * Add a string based search condition, referenced against one or more columns.
	 * Automatically escapes the passed data.  If multiple fieldnames are passed, it will perform an OR check against them all.
	 *
	 * @param string|array $fieldname The name(s) of the columns to test against. Multiple columns will be tested as an OR condition.
	 * @param string $value The data to test against
	 * @param boolean $operator optional equivalency operator to use in the test. Defaults to equal
	 * @return Query The current Query object.
	 */
	function whereString($fieldname, $value, $operator='=') {
		$param = $this->createParam($value);

		if (!is_array($fieldname)) $fieldname = array($fieldname);

		$out = array();
		foreach ($fieldname as $f) {
			$out[] = "$f $operator $param";
		}
		
		switch (count($out)) {
		case 0: 
			break;
		case 1: 
			$this->where[] = reset($out);
			break;
		default:
			$this->where[] = '('.implode(' OR ', $out).')';
			break;
		}
		
		return $this;
	}
	
	/**
	 * Syntactic sugar for whereString with a wildcard search
	 *
	 * @param string $fieldname 
	 * @param string $value 
	 * @return Query The current Query object.
	 */
	function whereStringLike($fieldname, $value) {
		return $this->whereString($fieldname, "%{$value}%", 'LIKE');
	}

	/**
	 * Syntactic sugar for whereString with negative condition
	 *
	 * @param string $fieldname 
	 * @param string $value 
	 * @return Query The current Query object.
	 */
	function whereStringNot($fieldname, $value) {
		return $this->whereString($fieldname, $value, '!=');
	}
	
	
	/**
	 * Add an integer based search condition, referenced against one or more columns.
	 * Automatically escapes the passed as an integer.  If multiple fieldnames are passed, it will perform an OR check against them all.
	 *
	 * @param string|array $fieldname The name(s) of the columns to test against. Multiple columns will be tested as an OR condition.
	 * @param integer $value The data to test against
	 * @param string $operator optional equivalency operator to use in the test. Defaults to equal
	 * @return Query The current Query object.
	 */
	function whereInteger($fieldname, $value, $operator='=') {
		return $this->whereDecimal($fieldname, $value, 0, $operator);
	}
	
	
	/**
	 * Syntactic sugar whereInteger, testing an unequal value
	 *
	 * @param string $fieldname 
	 * @param string $value 
	 * @return Query The current Query object.
	 */
	function whereIntegerNot($fieldname, $value) {
		return $this->whereInteger($fieldname, $value, '!=');
	}
	

	/**
	 * Add an integer based search condition, referenced against one or more columns.
	 * Automatically escapes the passed data as a decimal value.  If multiple fieldnames are passed, it will perform an OR check against them all.
	 *
	 * @param string|array $fieldname The name(s) of the columns to test against. Multiple columns will be tested as an OR condition.
	 * @param integer $from optional The start value as either a string or float.  The search will include this value.  If omitted, will search for values less than the $to value.
	 * @param integer $to optional The stop value as either a string or float.  The search will include this value.  If omitted, will search for value greater than the $from value.
	 * @return Query The current Query object.
	 */
	function whereIntegerInRange($fieldname, $from=null, $to=null) {
		return $this->whereDecimalInRange($fieldname, $from, $to, 0);
	}

	/**
	 * Add a boolean based search condition, referenced against one or more columns.
	 * Automatically escapes the passed value as a boolean.  If multiple fieldnames are passed, it will perform an OR check against them all.
	 *
	 * @param string|array $fieldname The name(s) of the columns to test against. Multiple columns will be tested as an OR condition.
	 * @param boolean $value The data to test against
	 * @return Query The current Query object.
	 */
	function whereBoolean($fieldname, $value = true) {
		$value = $value ? 'IS TRUE' : 'IS FALSE';
		
		if (!is_array($fieldname)) $fieldname = array($fieldname);

		$out = array();
		foreach ($fieldname as $f) {
			$out[] = "$f $value";
		}

		switch (count($out)) {
		case 0: 
			break;
		case 1: 
			$this->where[] = reset($out);
			break;
		default:
			$this->where[] = '('.implode(' OR ', $out).')';
			break;
		}

		return $this;
	}

	/**
	 * Add a boolean based search condition, referenced against one or more columns, testing for a value of true.
	 * Automatically escapes the passed value as a boolean.  If multiple fieldnames are passed, it will perform an OR check against them all.
	 *
	 * @param string|array $fieldname The name(s) of the columns to test against. Multiple columns will be tested as an OR condition.
	 * @return Query The current Query object.
	 */
	function whereTrue($fieldname) {
		return $this->whereBoolean($fieldname, true);
	}

	/**
	 * Add a boolean based search condition, referenced against one or more columns, testing for a value of false
	 * Automatically escapes the passed value as a boolean.  If multiple fieldnames are passed, it will perform an OR check against them all.
	 *
	 * @param string|array $fieldname The name(s) of the columns to test against. Multiple columns will be tested as an OR condition.
	 * @return Query The current Query object.
	 */
	function whereFalse($fieldname) {
		return $this->whereBoolean($fieldname, false);
	}

	/**
	 * Add a decimal based search condition, referenced against one or more columns.
	 * Automatically escapes the passed data as a decimal value.  If multiple fieldnames are passed, it will perform an OR check against them all.
	 *
	 * @param string|array $fieldname The name(s) of the columns to test against. Multiple columns will be tested as an OR condition.
	 * @param string|integer|float $value The data to test against
	 * @param integer $precision The number of decimals to display.  Defaults to 2.
	 * @param string $operator optional equivalency operator to use in the test. Defaults to equal
	 * @return Query The current Query object.
	 */
	function whereDecimal($fieldname, $value, $precision=2, $operator='=') {
		$param = $this->createParam(number_format($value, $precision, '.', ''));
		
		if (!is_array($fieldname)) $fieldname = array($fieldname);

		$out = array();
		foreach ($fieldname as $f) {
			$out[] = "$f $operator $param";
		}

		switch (count($out)) {
		case 0: 
			break;
		case 1: 
			$this->where[] = array_shift($out);
			break;
		default:
			$this->where[] = '('.implode(' OR ', $out).')';
			break;
		}

		return $this;
	}
	
	
	/**
	 * Syntactic sugar whereDecimal, testing an unequal value
	 *
	 * @param string $fieldname 
	 * @param string|float $value 
	 * @param integer $precision The number of decimals to display.  Defaults to 2.
	 * @return Query The current Query object.
	 */
	function whereDecimalNot($fieldname, $value, $precision=2) {
		return $this->whereDecimal($fieldname, $value, $precision, '!=');
	}
	

	/**
	 * Add a decimal based search condition, referenced against one or more columns.
	 * Automatically escapes the passed data as a decimal value.  If multiple fieldnames are passed, it will perform an OR check against them all.
	 *
	 * @param string|array $fieldname The name(s) of the columns to test against. Multiple columns will be tested as an OR condition.
	 * @param string|float $from optional The start value as either a string or float.  The search will include this value.  If omitted, will search for values less than the $to value.
	 * @param string|float $to optional The stop value as either a string or float.  The search will include this value.  If omitted, will search for value greater than the $from value.
	 * @param integer $precision The number of decimals to display.  Defaults to 2.
	 * @return Query The current Query object.
	 */
	function whereDecimalInRange($fieldname, $from=null, $to=null, $precision=2) {
		$param_from = $this->createParam(number_format($from, $precision, '.', ''));
		$param_to = $this->createParam(number_format($to, $precision, '.', ''));
		
		if (!is_array($fieldname)) $fieldname = array($fieldname);

		$out = array();
		foreach ($fieldname as $f) {
			if (!is_null($from) && !is_null($to)) {
				$out[] = "(`$f` >= $param_from AND `$fieldname` <= $param_to)";
			} elseif (!is_null($from)) {
				$out[] = "`$f` >= $param_from";
			} elseif (!is_null($to)) {
				$out[] = "`$f` <= $param_to";
			}
		}

		switch (count($out)) {
		case 0: 
			break;
		case 1: 
			$this->where[] = reset($out);
			break;
		default:
			$this->where[] = '('.implode(' OR ', $out).')';
			break;
		}


		return $this;
	}
	
	/**
	 * Add a date based search condition, referenced against one column.
	 * Automatically escapes the passed data as MySQL dates without any time information.
	 * This function does NOT support multiple columns.
	 *
	 * @param string $fieldname The name of the column to test against.
	 * @param string|integer $from optional The start date as either a string or a time() integer.  The search will include this day.  If omitted, will search for items prior to the $to value.
	 * @param string|integer $to optional The stop date as either a string or a time() integer.  The search will include this day.  If omitted, will search for items after the $from value.
	 * @return Query The current Query object.
	 */
	function whereDateInRange($fieldname, $from=0, $to=0) {
		if ($from && $from == $to) {

			$param_from = $this->createParam(static::ParseDate($from, static::PARSEDATE_DATE));
			$this->where[] = "DATE($fieldname) = DATE($param_from)";

		} elseif ($from && $to) {

			$param_from = $this->createParam(static::ParseDate($from, static::PARSEDATE_DATE));
			$param_to = $this->createParam(static::ParseDate($to, static::PARSEDATE_DATE));
			$this->where[] = "DATE($fieldname) BETWEEN DATE($param_from) AND DATE($param_to)";
			
		} elseif ($from) {
			
			$param_from = $this->createParam(static::ParseDate($from, static::PARSEDATE_DATE));
			$this->where[] = "DATE($fieldname) >= DATE($param_from)";
			
		} elseif ($to) {
			
			$param_to = $this->createParam(static::ParseDate($to, static::PARSEDATE_DATE));
			$this->where[] = "DATE($fieldname) <= DATE($param_to)";
			
		}
		
		return $this;
	}

	/**
	 * Add a time based search condition, referenced against one column.
	 * Automatically escapes the passed data as MySQL times without any date information.
	 * This function does NOT support multiple columns.
	 *
	 * @param string $fieldname The name of the column to test against.
	 * @param string|integer $from optional The start time as either a string or a time() integer.  If omitted, will search for items prior to the $to value.
	 * @param string|integer $to optional The stop time as either a string or a time() integer.  If omitted, will search for items after the $from value.
	 * @return Query The current Query object.
	 */
	function whereTimeInRange($fieldname, $from=0, $to=0) {
		if ($from && $from == $to) {
			
			$param_from = $this->createParam(static::ParseDate($from, static::PARSEDATE_TIME));
			$this->where[] = "TIME($fieldname) = TIME($param_from)";
			
		} elseif ($from && $to) {
			
			$param_from = $this->createParam(static::ParseDate($from, static::PARSEDATE_TIME));
			$param_to = $this->createParam(static::ParseDate($to, static::PARSEDATE_TIME));
			$this->where[] = "TIME($fieldname) BETWEEN TIME($param_from) AND TIME($param_to)";
			
		} elseif ($from) {
			
			$param_from = $this->createParam(static::ParseDate($from, static::PARSEDATE_TIME));
			$this->where[] = "TIME($fieldname) >= TIME($param_from)";
			
		} elseif ($to) {
			
			$param_to = $this->createParam(static::ParseDate($to, static::PARSEDATE_TIME));
			$this->where[] = "TIME($fieldname) <= TIME($param_to)";
			
		}
		
		return $this;
	}
	
	/**
	 * Add a datetime based search condition, referenced against one column.
	 * Automatically escapes the passed data as MySQL dates without any time information.
	 * This function does NOT support multiple columns.
	 *
	 * @param string $fieldname The name of the column to test against.
	 * @param string|integer $from optional The start date as either a string or a time() integer.  The search will include this day.  If omitted, will search for items prior to the $to value.
	 * @param string|integer $to optional The stop date as either a string or a time() integer.  The search will include this day.  If omitted, will search for items after the $from value.
	 * @return Query The current Query object.
	 */
	function whereDateTimeInRange($fieldname, $from=0, $to=0) {
		if ($from && $from == $to) {
			
			$param_from = $this->createParam(static::ParseDate($from, static::PARSEDATE_DATETIME));
			$this->where[] = "DATETIME($fieldname) = DATETIME($param_from)";
			
		} elseif ($from && $to) {
			
			$param_from = $this->createParam(static::ParseDate($from, static::PARSEDATE_DATETIME));
			$param_to = $this->createParam(static::ParseDate($to, static::PARSEDATE_DATETIME));
			$this->where[] = "DATETIME($fieldname) BETWEEN DATETIME($param_from) AND DATETIME($param_to)";
			
		} elseif ($from) {
			
			$param_from = $this->createParam(static::ParseDate($from, static::PARSEDATE_DATETIME));
			$this->where[] = "DATETIME($fieldname) >= DATETIME($param_from)";
			
		} elseif ($to) {
			
			$param_to = $this->createParam(static::ParseDate($to, static::PARSEDATE_DATETIME));
			$this->where[] = "DATETIME($fieldname) <= DATETIME($param_to)";
			
		}
		
		return $this;
	}
	
	/**
	 * Internal function for parsing passed date values into mysql compatible values
	 *
	 * @param string $input 
	 * @return void
	 * @author Jarvis Badgley
	 */
	const PARSEDATE_DATETIME = 0;
	const PARSEDATE_DATE = 1;
	const PARSEDATE_TIME = 2;
	protected static function ParseDate($input, $return) {
		if ($input) {
			if (!($input instanceof DateTime)) {
				if (is_string($input)) {
					if (strtolower($input) === 'now') {
						$input = new DateTime();
					} else {
						try {
							$input = new DateTime($input);
						} catch (Exception $e) {
							return null;
						}
					}
				} elseif (is_integer($input) && $input>0) {
					$input = new DateTime();
					$input->setTimestamp($input);
				} else {
					return null;
				}
			}
			switch ($return) {
			case 0: return $input->format('Y-m-d H:i:s'); 	//date and time
			case 1: return $input->format('Y-m-d');			//date only
			case 2: return $input->format('H:i:s');			//time only
			}
		}
		return null;
	}
	
	
	/**
	 * Add a condition to compare a column against a list of values
	 * Automatically escapes the passed values as MySQL strings.
	 * This function does NOT support multiple columns.
	 *
	 * @param string $fieldname The name of the column to test against.
	 * @param array $values Array of strings to search for
	 * @return Query The current Query object.
	 */
	function whereInList($fieldname, array $values, $not = false) {
		if (!is_array($values)) return $this;
		
		foreach ($values as $i=>$value) {
			$values[$i] = $this->createParam($value);
		}
		
		$not = $not ? 'NOT' : '';
		
		$this->where[] = "$fieldname $not IN (".implode(",", $values).")";	
		
		return $this;
	}


	/**
	 * Opposite of the whereInList function
	 *
	 * @param string $fieldname 
	 * @param array $values 
	 * @return void
	 * @author Jarvis Badgley
	 */
	function whereNotInList($fieldname, array $values) {
		return $this->whereInList($fieldname, $values, true);
	}
	
	
	/**
	 * Add a custom where clause
	 *
	 * @param string $where The condition to test
	 * @return Query The current Query object.
	 */
	function where($where, $data=null) {
		$this->where[] = $where;
		if ($data !== null) $this->parameters = array_merge($this->parameters, is_array($data) ? $data : array($data));
		return $this;
	}
	

/**
	SET VALUES
*/	
		
	function setString($fieldname, $value) {
		$this->set[] = "$fieldname = " . $this->createParam($value);
		return $this;
	}
	
	function setInteger($fieldname, $value) {
		return $this->setDecimal($fieldname, $value, 0);
	}
	
	function setDecimal($fieldname, $value, $precision=2) {
		$this->set[] = "$fieldname = " . $this->createParam(number_format($value, $precision, '.', ''));
		return $this;
	}
	
	function setDate($fieldname, $value) {
		$this->set[] = "$fieldname = " . $this->createParam(static::ParseDate($value, static::PARSEDATE_DATE));
		return $this;
	}
	
	function setTime($fieldname, $value) {
		$this->set[] = "$fieldname = " . $this->createParam(static::ParseDate($value, static::PARSEDATE_TIME));
		return $this;
	}
	
	function setDateTime($fieldname, $value) {
		$this->set[] = "$fieldname = " . $this->createParam(static::ParseDate($value, static::PARSEDATE_DATETIME));
		return $this;
	}

	function setBoolean($fieldname, $value) {
		$this->set[] = "$fieldname = " . ($value ? 'TRUE' : 'FALSE');
		return $this;
	}
	
	function setTrue($fieldname) {
		$this->setBoolean($fieldname, true);
		return $this;
	}

	function setFalse($fieldname) {
		$this->setBoolean($fieldname, false);
		return $this;
	}
	
	function set($set, $data=null) {
		$this->set[] = $set;
		if ($data !== null) $this->parameters = array_merge($this->parameters, is_array($data) ? $data : array($data));
		return $this;
	}
	
	
/**
	QUERY PARAMETERS
*/	
	
	
	
	/**
	 * Define the columns to be returned by the query.  Default value is *, returning all columns.
	 * This follows the same notation as if you were writing the query yourself and must follow any table aliases defined.  Can be a string or an array of strings.
	 *
	 * @param string|array $columns The column for the query to return in the database request.  
	 * @return Query The current Query object.
	 */
	function returns() {
		if (!func_num_args()) throw new QueryException("QueryBuilder#returnColumns: Columns cannot be null.");
		
		$columns = array();
		foreach (func_get_args() as $arg) {
			if (is_array($arg)) $columns = array_merge($columns, $arg);
			else $columns[] = $arg;			
		}
		$this->return = $columns;
		return $this;
	}


	/**
	 * Join an external table to the query.
	 *
	 * @param string $join The join to perform
	 * @return Query The current Query object.
	 */
	function join($join, $data=null) {
		$this->joins[] = $join;
		if ($data !== null) $this->parameters = array_merge($this->parameters, is_array($data) ? $data : array($data));
		return $this;
	}
	
	/**
	 * Inner Join an external table to the query.
	 *
	 * @param string $join The join to perform
	 * @return Query The current Query object.
	 */
	function innerJoin($join, $data=null) {
		return $this->join("INNER JOIN $join", $data);
	}

	/**
	 * Outer Join an external table to the query.
	 *
	 * @param string $join The join to perform
	 * @return Query The current Query object.
	 */
	function outerJoin($join, $data=null) {
		return $this->join("OUTER JOIN $join", $data);
	}
		
	
	/**
	 * Left Join an external table to the query.
	 *
	 * @param string $join The join to perform
	 * @return Query The current Query object.
	 */
	function leftJoin($join, $data=null) {
		return $this->join("LEFT JOIN $join", $data);
	}

	/**
	 * Right Join an external table to the query.
	 *
	 * @param string $join The join to perform
	 * @return Query The current Query object.
	 */
	function rightJoin($join, $data=null) {
		return $this->join("RIGHT JOIN $join", $data);
	}
	
	/**
	 * Changes the boolean condition used for combining where clauses.  Default is AND
	 *
	 * @param string|boolean $condition The new boolean condition.  Can be true/false or 'and'/'or'
	 * @return Query The current Query object.
	 */
	function inclusive($condition) {
		switch ($condition) {
		case true:
		case 'and':
		case 'AND':
		case 'yes':
			$this->boolean = 'AND'; break;
			
		case false:
		case 'or':
		case 'OR':
		case 'no':
			$this->boolean = 'OR'; break;
		}
		return $this;
	}
		
	/**
	 * Defines the order by condition
	 *
	 * @param string/array $columns Optional. The fields/conditions to order by as either a string or array of strings.  Supports multiple parameters for multiple order levels
	 * @return Query The current Query object.
	 */
	function orderBy() {
		$columns = array();
		foreach (func_get_args() as $arg) {
			if (is_array($arg)) $columns = array_merge($columns, $arg);
			else $columns[] = $arg;			
		}
		$this->orderby = implode(',',$columns);
		return $this;
	}
	
	/**
	 * Sets if the results should be distinct
	 *
	 * @param boolean $on
	 * @return Query The current Query object.
	 */
	function distinct($on=true) {
		$this->distinct = $on;
		return $this;
	}

	/**
	 * Defines the group by condition
	 *
	 * @param string/array $columns Optional. The fields/conditions to group by as either a string or array of strings. Supports multiple parameters for multiple grouping levels
	 * @return Query The current Query object.
	 */
	function groupBy() {
		$columns = array();
		foreach (func_get_args() as $arg) {
			if (is_array($arg)) $columns = array_merge($columns, $arg);
			else $columns[] = $arg;			
		}
		$this->groupby = implode(',',$columns);
		return $this;
	}

	/**
	 * Defines the limit on the number of rows to return.  This value is ignored by the buildCountQuery() and count() functions.
	 * If both arguments are omitted, the limit is removed.
	 *
	 * @param integer $max optional Maximum number of rows to return.
	 * @param integer $start optional The index of the first result to return.
	 * @return Query The current Query object.
	 */
	function limit($max=0, $start=0) {
		$this->limit = $max?"LIMIT ".(int)$start.', '.(int)$max:'';
		return $this;
	}
	

/**
	QUERY BUILDING
*/
	
	/**
	 * Creates the search query for fetching the results.
	 *
	 * @param boolean $debug Optional. If true, function will return a unified string using the Unprepare function.  SHOULD ONLY BE USED FOR DEBUGGING AND HASHING PURPOSES
	 * @return string MySQL Query
	 */
	function buildSelect($debug=false) {
		$columns = implode(', ',$this->return);
		if ($this->distinct) $columns = "DISTINCT ".$columns;

		$q = array("SELECT {$columns}","FROM {$this->table}");
		foreach ($this->joins as $j) $q[] = $j;
		
		if (!empty($this->where)) {
			$q[] = "WHERE " . implode(" {$this->boolean} ", $this->where);
		}
		
		if ($this->groupby) $q[] = "GROUP BY {$this->groupby}";
		if ($this->orderby) $q[] = "ORDER BY {$this->orderby}";
		if ($this->limit) $q[] = $this->limit;
		
		if ($debug) return $this->unprepare(implode("\n", $q), $this->parameters);
		return array(implode(' ', $q), $this->parameters);
	}
	
	
	/**
	 * Creates the search query for fetching the total number of results.
	 * This function ignores the limit directive.
	 *
	 * @param boolean $debug Optional. If true, function will return a unified string using the Unprepare function. SHOULD ONLY BE USED FOR DEBUGGING AND HASHING PURPOSES
	 * @return string MySQL Query
	 */
	function buildCount($debug=false) {
		
		$distinct = ($this->distinct && is_string($this->distinct))?"DISTINCT {$this->distinct}":'*';
		$q = array("SELECT count({$distinct})","FROM {$this->table}");
		foreach ($this->joins as $j) $q[] = $j;
		
		if (!empty($this->where)) {
			$q[] = "WHERE " . implode(" {$this->boolean} ", $this->where);
		}
		
		if ($this->groupby) $q[] = "GROUP BY {$this->groupby}";
		
		if ($debug) return $this->unprepare(implode("\n", $q), $this->parameters);
		return array(implode(' ', $q), $this->parameters);
	}
	
	
	/**
	 * Creates the delete query for removing rows.
	 *
	 * @param boolean $debug Optional. If true, function will return a unified string using the Unprepare function. SHOULD ONLY BE USED FOR DEBUGGING AND HASHING PURPOSES
	 * @return string MySQL Query
	 */
	function buildDelete($debug=false) {
		$columns = implode(', ',$this->return);
		if ($columns=='*') $columns = '';

		$q = array("DELETE {$columns}","FROM {$this->table}");

		foreach ($this->joins as $j) $q[] = $j;
		
		if (!empty($this->where)) {
			$q[] = "WHERE " . implode(" {$this->boolean} ", $this->where);
		}
				
		if ($debug) return $this->unprepare(implode("\n", $q), $this->parameters);
		return array(implode(' ', $q), $this->parameters);
	}

	/**
	 * Creates the delete query for removing rows.
	 *
	 * @param boolean $debug Optional. If true, function will return a unified string using the Unprepare function. SHOULD ONLY BE USED FOR DEBUGGING AND HASHING PURPOSES
	 * @return string MySQL Query
	 */
	function buildInsert($debug=false) {
		$q = array("INSERT INTO {$this->table}");

		if (!empty($this->set)) {
			$q[] = "SET " . implode(", ", $this->set);
		}
				
		if ($debug) return $this->unprepare(implode("\n", $q), $this->parameters);
		return array(implode(' ', $q), $this->parameters);
	}


	/**
	 * Creates the delete query for removing rows.
	 *
	 * @param boolean $debug Optional. If true, function will return a unified string using the Unprepare function. SHOULD ONLY BE USED FOR DEBUGGING AND HASHING PURPOSES
	 * @return string MySQL Query
	 */
	function buildUpdate($debug=false) {
		$q = array("UPDATE {$this->table}");
		foreach ($this->joins as $j) $q[] = $j;
		
		if (!empty($this->set)) {
			$q[] = "SET " . implode(", ", $this->set);
		}

		if (!empty($this->where)) {
			$q[] = "WHERE " . implode(" {$this->boolean} ", $this->where);
		}
				
		if ($debug) return $this->unprepare(implode("\n", $q), $this->parameters);
		return array(implode(' ', $q), $this->parameters);
	}

/**
	QUERY EXECUTION
*/	

	/**
	 * Performs the search query using the Database object and returns the results in the format specified.
	 * 	Query::RETURN_NONE            Returns the number of affected rows.
	 * 	Query::RETURN_FULL            Returns an indexed array of column named arrays for all rows.
	 * 	Query::RETURN_SINGLE_ROW      Returns a column named array of the first row fetched
	 * 	Query::RETURN_SINGLE_COLUMN   Returns an indexed array of all results in the first column for each row
	 * 	Query::RETURN_SINGLE_CELL     Returns a string containing the first column of the first row
	 *  
	 * 	May also pass a class name or class instance.  The result will return an array of the specified class, passing each result row to the class constructor.
	 *
	 * @param integer|string|object $format Optional return format condition.
	 * @return array
	 */
	function select($format=self::RETURN_FULL) {
		list($query, $data) = $this->buildSelect();
		
		$pdo = $this->getPDOLink();
		
		$result = $pdo->prepare($query);
		if (empty($data) ? $result->execute() : $result->execute($data)) {
		
			$c = $result->rowCount();
		
			if (is_integer($format)) {		
				switch ((int)$format) {
					case self::RETURN_NONE:
						return $c;
			
					case self::RETURN_SINGLE_ROW:
						return $c ? $result->fetch(PDO::FETCH_ASSOC):array();
				
					case self::RETURN_SINGLE_COLUMN:
						return $c ? $result->fetchAll(PDO::FETCH_COLUMN, 0):array();
				
					case self::RETURN_SINGLE_CELL:
						if ($c) {
							$row = $result->fetch(PDO::FETCH_NUM);
							return $row[0];
						} else {
							return null;
						}
					
					case self::RETURN_FULL:
					default:
						return $c ? $result->fetchAll(PDO::FETCH_ASSOC):array();
				}
			} elseif (is_object($format) || class_exists($format)) {
				$rows = $c ? $result->fetchAll(PDO::FETCH_ASSOC):array();
				array_walk($rows, function (&$item, $index) use ($format) {
					$item = new $format($item);
				});
				return $rows;
			}
			
		}
	}
	
	/**
	 * Performs the search query using the Database object and returns the total number of results.
	 *
	 * @return integer
	 */
	function count() {
		list($query, $data) = $this->buildCount();
		
		$pdo = $this->getPDOLink();
		
		$result = $pdo->prepare($query);
		if ((empty($data) ? $result->execute() : $result->execute($data)) && $result->rowCount()) {
			$row = $result->fetch(PDO::FETCH_NUM);
			return (int)$row[0];
		} else {
			return null;
		}
	}
	
	/**
	 * Performs the search query using the Database object and returns the total number of results.
	 *
	 * @return integer
	 */
	function delete() {
		list($query, $data) = $this->buildDelete();
		
		$pdo = $this->getPDOLink();
		
		$result = $pdo->prepare($query);
		if (empty($data) ? $result->execute() : $result->execute($data)) {
			return $result->rowCount();
		} else {
			return false;
		}
	}

	/**
	 * Performs an insert and returns the total number of results.
	 *
	 * @return integer
	 */
	function insert() {
		list($query, $data) = $this->buildInsert();
		
		$pdo = $this->getPDOLink();
		
		$result = $pdo->prepare($query);
		if (empty($data) ? $result->execute() : $result->execute($data)) {
			return $pdo->lastInsertId() ?: true;
		} else {
			return false;
		}
	}
	
	/**
	 * Performs an insert using the defined select criteria
	 *
	 * @param string $target 
	 * @param string $column 
	 * @return void
	 * @author Jarvis Badgley
	 */
	function selectInto($target, $column) {
		list($query, $data) = $this->buildSelect();
		
		$args = func_get_args();
		
		array_shift($args); //pop off the table name
				
		$query = "INSERT INTO `$target` (" . implode(',',$args) . ") $query";
		
		$pdo = $this->getPDOLink();
		
		$result = $pdo->prepare($query);
		if (empty($data) ? $result->execute() : $result->execute($data)) {
			return $pdo->lastInsertId() ?: true;
		} else {
			return false;
		}		
	}

	/**
	 * Performs an update and returns the total number of rows affected
	 *
	 * @return integer
	 */
	function update() {
		list($query, $data) = $this->buildUpdate();
		
		$pdo = $this->getPDOLink();
		
		$result = $pdo->prepare($query);
		if (empty($data) ? $result->execute() : $result->execute($data)) {
			return $result->rowCount();
		} else {
			return false;
		}
	}

	
/**
	INTERNALLY USED FUNCTIONS
*/	
	
	
	/**
	 * Combines a prepared query and data array to return unified query string.
	 * THIS FUNCTION IS FOR DEBUG PURPOSES ONLY, NEVER USE THIS IN REAL CODE
	 *
	 * @param string $query 
	 * @param string $data 
	 * @return string
	 */
	protected function unprepare($query, $data=null) {
		$pdo = $this->getPDOLink();
		
		if (!is_array($data) || empty($data)) {
			return $query;
		}
		
		foreach ($data as $key=>$value) {
			$query = str_replace($key, $pdo->quote($value), $query);
		}
				
		return $query;
	}
	
	

}

class QueryException extends \Exception {}
