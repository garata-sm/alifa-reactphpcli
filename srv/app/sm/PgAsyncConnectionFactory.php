<?php
namespace SM;
use PgAsync\Client;
class PgAsyncConnectionFactory {
	/**
	 * singleton trait
	 **/
	use SingletonTrait;
	/**
	 * This class will host a single client db connection
	 **/
	protected $client;
	/**
	 * create a new connection
	 **/
	protected function __construct($parameters, $loop){
		$this->client = new Client($parameters, $loop); 
	}
}
