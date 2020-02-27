<?php
use GetOpt\GetOpt;
use GetOpt\Option;
use GetOpt\Operand;
use GetOpt\Command;
use GetOpt\ArgumentException;
use GetOpt\ArgumentException\Missing;
use SM\Reactor;
require_once __DIR__ . '/vendor/autoload.php';
define('ISCLI', PHP_SAPI === 'cli');
define('NAME', 'ReactPHP Daemon');
define('VERSION', '1.0-alpha');
#echo "Current PHP version: " . phpversion() . (ISCLI ? '(cli)' : '')  . "\n";
#echo $argc . " " .  $argv[1] . "\n:";
function get_host_from_uri($uri) {
    $ip = trim($uri);
    $is_ip = false;
    if (preg_match("/^[0-9]{1,3}(.[0-9]{1,3}){3}$/", $ip)) {
        $is_ip = true;
        foreach (explode(".", $ip) as $block)
            if( $block < 0 || $block > 255 )
                $is_ip = false;
    }
    if (!$is_ip)
        return gethostbyname($uri);
    return $uri;
}
function sanitize_postgres_uri($uri) {
    if ( substr( $uri, 0, 8 + 3 ) !== "postgres://" ) {
        $uri = "postgres://" . $uri;
    }
    $parsed_uri = parse_url($uri);
    if (array_key_exists ("path", $parsed_uri)) {
        $parsed_uri["path"] = substr($parsed_uri["path"], 1, strlen($parsed_uri["path"]) - 1);
    }
    return $parsed_uri;
}
function sanitize_redis_uri($uri) {
    if ( substr( $uri, 0, 5 + 3 ) !== "redis://"  ) {
        $uri = "redis://" . $uri;
    }
    $parsed_uri = parse_url($uri);
    if (array_key_exists ("path", $parsed_uri)) {
        $parsed_uri["path"] = substr($parsed_uri["path"], 1, strlen($parsed_uri["path"]) - 1);
    }
    return $parsed_uri;
}
$getOpt = new GetOpt();
// define common options
$getOpt->addOptions([
   
    Option::create(null, 'ver', GetOpt::NO_ARGUMENT)
        ->setDescription('Show version information and quit'),
        
    Option::create('?', 'help', GetOpt::NO_ARGUMENT)
        ->setDescription('Show this help and quit'),
    Option::create(null, 'postgres_uri', GetOpt::REQUIRED_ARGUMENT)
        ->setDescription('Set PostgreSQL complete URI'),
    Option::create(null, 'redis_uri', GetOpt::REQUIRED_ARGUMENT)
        ->setDescription('Set REDIS complete URI'),
    Option::create(null, 'redis_tls', GetOpt::OPTIONAL_ARGUMENT)
        ->setDescription('Set REDIS tls as per https://www.iana.org/assignments/uri-schemes/prov/rediss; assume "0" (ie, zero) as default value')
]);
$channel_op = new Operand('channel_op', Operand::MULTIPLE);
$getOpt->addOperands([$channel_op]);
$getOpt->addCommand(Command::create('test-setup', function () { 
    //echo $pgsqlhost . " " . $pgsqlport . " " . $pgsqluser . " " . $pgsqlpass . " " . $pgsqldb . " " . $redishost . " " . $redisport  . " "  . $redischannel . PHP_EOL;
})->setDescription('Check if setup works'));
// add commands
#$getOpt->addCommand(new CopyCommand());
#$getOpt->addCommand(new MoveCommand());
#$getOpt->addCommand(new DeleteCommand());
// process arguments and catch user errors
try {
    try {
        $getOpt->process();
    } catch (Missing $exception) {
        // catch missing exceptions if help is requested
        if (!$getOpt->getOption('help')) {
            throw $exception;
        }
    }
} catch (ArgumentException $exception) {
    file_put_contents('php://stderr', $exception->getMessage() . PHP_EOL);
    echo PHP_EOL . $getOpt->getHelpText();
    exit;
}
$redisuri = $getOpt->getOption('redis_uri');
$postgresuri = $getOpt->getOption('postgres_uri');
$redistls = $getOpt->getOption('redis_tls') ?: 0;
$redischannel = $getOpt->getOperand('channel_op');
if ($redisuri && $postgresuri && is_array($redischannel)) {
    $redisuri_ = sanitize_redis_uri($redisuri);
    $postgresuri_ = sanitize_postgres_uri($postgresuri);
    $pgsqlhost = $postgresuri_["host"];
    $pgsqlport = $postgresuri_["port"];
    $pgsqluser = $postgresuri_["user"];
    $pgsqlpass = $postgresuri_["pass"];
    $pgsqldb = $postgresuri_["path"]; // path within the URI equals to DB name
    $redishost = $redisuri_["host"];
    $redisport = $redisuri_["port"];
    $redisuser = $redisuri_["user"];
    $redispass = $redisuri_["pass"];
    //$redishost = get_host_from_uri($redishost);
    //$pgsqlhost = get_host_from_uri($pgsqlhost);
    
    $redisconnection = array("user" => $redisuser, "password" => $redispass, "host" => $redishost, "port" => $redisport, "tls" => $redistls);
    $pgsqlconnection = array("user" => $pgsqluser, "password" => $pgsqlpass, "host" => $pgsqlhost, "port" => $pgsqlport, "database" => $pgsqldb);
    $reactor = new Reactor($pgsqlconnection, $redisconnection, $redischannel);
} else {
    // show version and quit
    if ($getOpt->getOption('ver')) {
        echo sprintf('%s: %s' . PHP_EOL, NAME, VERSION);
        exit;
    }
    // show help and quit
    $command = $getOpt->getCommand();
    if (!$command || $getOpt->getOption('help')) {
        echo $getOpt->getHelpText();
        exit;
    }
    // call the requested command
    //call_user_func($command->getHandler(), $getOpt);
}
