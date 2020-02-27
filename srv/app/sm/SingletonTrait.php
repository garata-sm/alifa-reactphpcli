<?php
namespace SM;
trait SingletonTrait {
	protected static $inst = null;
	/**
	 * call this method to get instance
	 */
	public static function getInstance($parameters, $loop) {
		if (static::$inst === null) {
			static::$inst = new static($parameters, $loop);
		}
		return static::$inst;
	}
	/**
	 * All property accessible from outside but readonly
	 * if property does not exist return null
	 *
	 * @param string $name
	 *
	 * @return mixed|null
	 */
	public function __get ($name) {
        	return $this->$name ?? null;
	}
	/**
	 * __set trap, property not writeable
	 *
	 * @param string $name
	 * @param mixed $value
	 *
	 * @return mixed
	 */
	function __set ($name, $value) {
        	return $value;
	}
	/**
	 * protected to prevent clonning 
	 */
	protected function __clone() {
	}
	/**
	 * protected so no one else can instance it 
	 */
	protected function __construct() {
	}
}
