<?php
namespace SM;
use Clue\React\Block;
use Clue\React\Redis\Client;
use Clue\React\Redis\Factory;
use Clue\React\Redis\StreamingClient;
use React\EventLoop\StreamSelectLoop;
use React\Promise\Promise;
use React\Promise\Deferred;
use React\Promise\RejectedPromise;
use React\Stream\DuplexResourceStream;
use React\Dns\Query\ExecutorInterface;
use React\Dns\Resolver\Resolver;
use React\Socket\Connector;
class Reactor
{
    const E_PRIORITY = '0';
    const H_PRIORITY = '1';
    const M_PRIORITY = '2';
    const L_PRIORITY = '3';
    const CHANNEL_PREFIX = 'dljobs';
    protected $con;
    protected $loop;
    protected $db;
    protected $client;
    protected $target;
    protected $factory;
    /**
     * Create a new ReactPHP governor instance.
     *
     * @return void
     */
    public function __construct($pgsqlconnection, $redisconnection, $redischan)
    {
        $this->loop = \React\EventLoop\Factory::create();
        $this->factory = new Factory($this->loop);
        $pgsqlhost = $pgsqlconnection["host"];
        $pgsqlport = $pgsqlconnection["port"];
        $pgsqluser = $pgsqlconnection["user"];
        $pgsqlpass = $pgsqlconnection["password"];
        $pgsqldb = $pgsqlconnection["database"];
        $this->con = PgAsyncConnectionFactory::getInstance([
            "host" => $pgsqlhost,
            "port" => $pgsqlport,
            "user" => $pgsqluser,
            "password" => $pgsqlpass,
            "database" => $pgsqldb
        ], $this->loop);
        $this->db = $this->con->client;
        $redisusr = $redisconnection["user"];
        $redispsw = $redisconnection["password"];
        $redishost = $redisconnection["host"];
        $redisport = $redisconnection["port"];
        $redistls = $redisconnection["tls"];
        $this->client = $this->connectToRedis( $this->factory, $redisusr, $redispsw, $redishost, $redisport, $redistls, $connected );
        echo $redishost, " ", $redisport, " ", $redisusr, "\n";
        echo $pgsqlhost, " ", $pgsqlport, " ", $pgsqluser, " ",  $pgsqldb, "\n";
        if ( $connected ) {
            $c = 0;
            while ($c < count($redischan)) {
                $this->client->subscribe($redischan[$c++]);
            }
            $self = $this;
            echo "Listening on [", implode(",", $redischan), "] REDIS channel(s)\n";
            $this->client->on('message', function ($channel, $payload) use (&$self) {
                $mtstart = microtime(TRUE);
                if ( strcasecmp(sprintf("%s%s", $self::CHANNEL_PREFIX, $self::E_PRIORITY), $channel) === 0 ) {
                    $self->dequeueErrorMessage($this->factory, $channel, $payload);
                } else {
                    $self->dequeueRegularMessage($channel, $payload, $mtstart);
                }
            });
        }
        $this->loop->run();
    }
    private function displayLogToStdOut($message) {
        $mtime = microtime(true);
        echo "$mtime $message\n";
    }
    private function dequeueRegularMessage( $channel, $payload, $mtstart = null )
    {
        $payload_decoded = json_decode($payload, true);
        $json_last_error = json_last_error();
        if ($json_last_error === JSON_ERROR_NONE) {
            $payload_id = array_shift($payload_decoded);
            $payload_data = array_shift($payload_decoded);
            if ($payload_id["status"] > 0) {
            }
            for ($i = 0; is_array($payload_data) && $i < count($payload_data); $i++) {
                $mtftp = $payload_id["timestamp"];
                $filename = $payload_id["filename"];
                $vuser = $payload_id["vuser"];
                $userid = $payload_id["userid"];
                $message = $payload_data[$i];
                $self = $this;
                $this->db->executeStatement('INSERT INTO transfers (message, filename, vuser) VALUES ($1, $2, $3)', 
                    [$message, $filename, $vuser])->subscribe(
                    function ($row) {
                    },
                    function ($e) {
                        echo "Failed.\n";
                    },
                    function () use ($message, $filename, $userid, $mtstart, $mtftp, &$self) {
                        #$t = microtime(true);
                        #$micro = sprintf("%06d",($t - floor($t)) * 1000000);
                        #$timestamp_now = new DateTime( date('Y-m-d H:i:s.' . $micro, $t) );
                        #$timestamp_now->setTimezone(new DateTimeZone("Europe/Rome"));
                        #$microend = microtime(TRUE);
                        #$md = round( ($microend - $mtftp) * 1000, 2 );
                        #$mt = round( ($microend - ($mtstart ?? 0)) * 1000, 2 );
                        #echo $filename, "\t",  $mtftp, "\t", $microend, "\t", $md, "\t",  $mt, "\n";
                        $headregex = '/^(ST[0-9]+),[6],([0-9]{2})\.([0-9]{2})\.([0-9]{2}),([0-9]{2}),([0-9]{2})[^0-9]([0-9]{4})[^0-9][0-9].*$/';
                        preg_match($headregex, $message, $matches);
                        $station = $matches[1];
                        $hour = str_pad($matches[2], 2, '0', STR_PAD_LEFT);
                        $minute = str_pad($matches[3], 2, '0', STR_PAD_LEFT);
                        $second = str_pad($matches[4], 2, '0', STR_PAD_LEFT);
                        $day = str_pad($matches[5], 2, '0', STR_PAD_LEFT);
                        $month = str_pad($matches[6], 2, '0', STR_PAD_LEFT);
                        $year = $matches[7];
                        $split = preg_split('/\b(?=[0-9]+,[B],[0-9.]+(?=,[R],[0-9]+)?)\b/u', $message); 
                        $is_valid_date = $minute === '00' || $minute === '15' || $minute === '30' || $minute === '45';
                        for ($i = 1, $len = count($split); $is_valid_date && $i < $len; $i++) {
                            $ssplit =  explode(',', $split[$i]);
                            $sublen = count($ssplit);
                            
                            $measurements = null;
                            $m_value = null;
                            $m_error = null;
                            $j = 0;
                            
                            $m_id = $ssplit[$j++];
                            $k = count($measurements) - 1;
                            if ('B' === $ssplit[$j++]) {
                                $m_value = $ssplit[$j];
                                if ($sublen > ($j + 1) && 'R' === $ssplit[$j + 1]) {
                                    $m_error = $ssplit[($j += 2)];
                                } else {
                                    $m_error = 0;
                                }
                            }
                            
                            $measurements = array("id" => $m_id, "value" => $m_value, "error" => $m_error);
                            # Exploit measurements_unique_trinary_key (station_id + measure_id + measurement_date) unique index with PostgreSQL 9.5 INSERT ... ON CONFLICT UPDATE statement
                            $self->db->executeStatement(sprintf('WITH T AS ( '.
                            'SELECT s.id AS station_id, m.id AS measure_id, s.user_id AS owner_id, '.
                            'TO_TIMESTAMP(\'%s\', \'YYYY-MM-DD hh24:mi:ss\')::timestamp at time zone s.timezone AS measurement_date, '.
                            '($1)::float AS measurement_data, ($2)::int AS error_code FROM stations AS s, measures AS m '.
                            'WHERE m.id = CAST($3 AS BIGINT) AND s.id = m.station_id AND s.user_id = CAST($4 AS BIGINT) AND s.name = $5) '.
                            'INSERT INTO measurements (station_id, measure_id, owner_id, measurement_date, measurement_data, error_code) '.
                            'SELECT T.station_id, T.measure_id, T.owner_id, T.measurement_date, T.measurement_data, T.error_code '.
                            'FROM T '.
                            'ON CONFLICT (station_id, measure_id, measurement_date) '.
                            'DO UPDATE SET (owner_id, measurement_date, measurement_data, error_code) = (SELECT T.owner_id, T.measurement_date, T.measurement_data, T.error_code FROM T)',
                            "$year-$month-$day $hour:$minute:$second"),
                            [
                                $measurements['value'], $measurements["error"], $measurements["id"], $userid, $station
                            ])->subscribe(
                                function($row) {
                                },
                                function($e) use ($self) {
                                    $self->displayLogToStdOut($e);
                                },
                                function() use($self, $measurements, $station, $userid, $year, $month, $day, $hour, $minute, $second) {
                                    $self->displayLogToStdOut(sprintf('The measure with %s ID for the \'%s\' station owned by %s user ID has been upserted correctly', $measurements["id"], $station, $userid));
                                    $self->db->executeStatement(sprintf('WITH T AS ( '.
                                    'SELECT s.id AS station_id, m.id AS measure_id, s.user_id AS owner_id, '.
                                    'TO_TIMESTAMP(\'%s\', \'YYYY-MM-DD hh24:mi:ss\')::timestamp at time zone s.timezone AS actual_measurement_date '.
                                    'FROM stations AS s, measures AS m '.
                                    'WHERE m.id = CAST($1 AS BIGINT) AND s.id = m.station_id AND s.user_id = CAST($2 AS BIGINT) AND s.name = $3) '.
                                    'UPDATE measurements '.
                                    'SET measurement_date_1hour = DATE_TRUNC(\'hour\'::text, measurement_date - \'00:00:00.5\'::interval) + \'01:00:00\'::interval, '.
                                    'measurement_date_3hours = CASE DATE_PART(\'hour\'::text, measurement_date - \'00:00:00.5\'::interval) '.
                                    'WHEN 0 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'03:00:00\'::interval '.
                                    'WHEN 1 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'03:00:00\'::interval '.
                                    'WHEN 2 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'03:00:00\'::interval '.
                                    'WHEN 3 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'06:00:00\'::interval '.
                                    'WHEN 4 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'06:00:00\'::interval '.
                                    'WHEN 5 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'06:00:00\'::interval '.
                                    'WHEN 6 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'09:00:00\'::interval '.
                                    'WHEN 7 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'09:00:00\'::interval '.
                                    'WHEN 8 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'09:00:00\'::interval '.
                                    'WHEN 9 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'12:00:00\'::interval '.
                                    'WHEN 10 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'12:00:00\'::interval '.
                                    'WHEN 11 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'12:00:00\'::interval '.
                                    'WHEN 12 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'15:00:00\'::interval '.
                                    'WHEN 13 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'15:00:00\'::interval '.
                                    'WHEN 14 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'15:00:00\'::interval '.
                                    'WHEN 15 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'18:00:00\'::interval '.
                                    'WHEN 16 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'18:00:00\'::interval '.
                                    'WHEN 17 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'18:00:00\'::interval '.
                                    'WHEN 18 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'21:00:00\'::interval '.
                                    'WHEN 19 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'21:00:00\'::interval '.
                                    'WHEN 20 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'21:00:00\'::interval '.
                                    'WHEN 21 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'1 day\'::interval '.
                                    'WHEN 22 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'1 day\'::interval '.
                                    'WHEN 23 THEN DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) + \'1 day\'::interval '.
                                    'ELSE NULL::timestamptz END, '.
                                    'measurement_date_24hours = DATE_TRUNC(\'day\'::text, measurement_date - \'00:00:00.5\'::interval), '.
                                    'measurement_date_10days = CASE date_part(\'day\'::text, measurement_date - \'00:00:00.5\'::interval) '.
                                    'WHEN 1 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'9 days\'::interval '.
                                    'WHEN 2 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'9 days\'::interval '.
                                    'WHEN 3 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'9 days\'::interval '.
                                    'WHEN 4 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'9 days\'::interval '.
                                    'WHEN 5 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'9 days\'::interval '.
                                    'WHEN 6 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'9 days\'::interval '.
                                    'WHEN 7 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'9 days\'::interval '.
                                    'WHEN 8 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'9 days\'::interval '.
                                    'WHEN 9 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'9 days\'::interval '.
                                    'WHEN 10 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'9 days\'::interval '.
                                    'WHEN 11 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'19 days\'::interval '.
                                    'WHEN 12 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'19 days\'::interval '.
                                    'WHEN 13 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'19 days\'::interval '.
                                    'WHEN 14 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'19 days\'::interval '.
                                    'WHEN 15 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'19 days\'::interval '.
                                    'WHEN 16 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'19 days\'::interval '.
                                    'WHEN 17 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'19 days\'::interval '.
                                    'WHEN 18 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'19 days\'::interval '.
                                    'WHEN 19 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'19 days\'::interval '.
                                    'WHEN 20 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'19 days\'::interval '.
                                    'WHEN 21 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'1 mon\'::interval - \'1 day\'::interval '.
                                    'WHEN 22 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'1 mon\'::interval - \'1 day\'::interval '.
                                    'WHEN 23 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'1 mon\'::interval - \'1 day\'::interval '.
                                    'WHEN 24 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'1 mon\'::interval - \'1 day\'::interval '.
                                    'WHEN 25 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'1 mon\'::interval - \'1 day\'::interval '.
                                    'WHEN 26 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'1 mon\'::interval - \'1 day\'::interval '.
                                    'WHEN 27 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'1 mon\'::interval - \'1 day\'::interval '.
                                    'WHEN 28 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'1 mon\'::interval - \'1 day\'::interval '.
                                    'WHEN 29 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'1 mon\'::interval - \'1 day\'::interval '.
                                    'WHEN 30 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'1 mon\'::interval - \'1 day\'::interval '.
                                    'WHEN 31 THEN DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) + \'1 mon\'::interval - \'1 day\'::interval '.
                                    'ELSE NULL::timestamptz END, '.
                                    'measurement_date_1month = DATE_TRUNC(\'month\'::text, measurement_date - \'00:00:00.5\'::interval), '.
                                    'measurement_date_3months = CASE DATE_PART(\'month\'::text, measurement_date - \'00:00:00.5\'::interval) '.
                                    'WHEN 1 THEN DATE_TRUNC(\'year\'::text, measurement_date - \'00:00:00.5\'::interval) + \'3 mons\'::interval - \'1 day\'::interval '.
                                    'WHEN 2 THEN DATE_TRUNC(\'year\'::text, measurement_date - \'00:00:00.5\'::interval) + \'3 mons\'::interval - \'1 day\'::interval '.
                                    'WHEN 3 THEN DATE_TRUNC(\'year\'::text, measurement_date - \'00:00:00.5\'::interval) + \'3 mons\'::interval - \'1 day\'::interval '.
                                    'WHEN 4 THEN DATE_TRUNC(\'year\'::text, measurement_date - \'00:00:00.5\'::interval) + \'6 mons\'::interval - \'1 day\'::interval '.
                                    'WHEN 5 THEN DATE_TRUNC(\'year\'::text, measurement_date - \'00:00:00.5\'::interval) + \'6 mons\'::interval - \'1 day\'::interval '.
                                    'WHEN 6 THEN DATE_TRUNC(\'year\'::text, measurement_date - \'00:00:00.5\'::interval) + \'6 mons\'::interval - \'1 day\'::interval '.
                                    'WHEN 7 THEN DATE_TRUNC(\'year\'::text, measurement_date - \'00:00:00.5\'::interval) + \'9 mons\'::interval - \'1 day\'::interval '.
                                    'WHEN 8 THEN DATE_TRUNC(\'year\'::text, measurement_date - \'00:00:00.5\'::interval) + \'9 mons\'::interval - \'1 day\'::interval '.
                                    'WHEN 9 THEN DATE_TRUNC(\'year\'::text, measurement_date - \'00:00:00.5\'::interval) + \'9 mons\'::interval - \'1 day\'::interval '.
                                    'WHEN 10 THEN DATE_TRUNC(\'year\'::text, measurement_date - \'00:00:00.5\'::interval) + \'1 year\'::interval - \'1 day\'::interval '.
                                    'WHEN 11 THEN DATE_TRUNC(\'year\'::text, measurement_date - \'00:00:00.5\'::interval) + \'1 year\'::interval - \'1 day\'::interval '.
                                    'WHEN 12 THEN DATE_TRUNC(\'year\'::text, measurement_date - \'00:00:00.5\'::interval) + \'1 year\'::interval - \'1 day\'::interval '.
                                    'ELSE NULL::timestamptz END '.
                                    'FROM T '.
                                    'WHERE measurements.measure_id = T.measure_id AND measurements.owner_id = T.owner_id '.
                                    'AND measurements.measurement_date = T.actual_measurement_date AND measurements.station_id = T.station_id ',
                                      "$year-$month-$day $hour:$minute:$second"), [
                                        $measurements["id"], $userid, $station
                                    ])->subscribe(
                                      function($row) {
                                
                                      },
                                      function($e) use ($self) {
                                        $self->displayLogToStdOut($e);
                                      },
                                      function() use ($self) {
                                      }
                                    );
                                }
                            );
                            
                            #$self->db->executeStatement('INSERT INTO measurements (station_id, measure_id, owner_id, measurement_date, measurement_data, error_code) '.
                            #    sprintf('SELECT s.id AS station_id, m.id AS measure_id, s.user_id, CONCAT(\'%s\', \' \', s.timezone)::timestamp AS measurement_date, $1 AS measurement_data, $2 AS error_code '.
                            #    'FROM stations AS s, measures AS m WHERE m.id = $3 AND s.id = m.station_id AND s.user_id = $4 AND s.name = $5', "$year-$month-$day $hour:$minute:$second"), [
                            #        floatval($measurements['value']), $measurements["error"], intval($measurements["id"]), intval($userid), $station
                            #])->subscribe(
                            #    function($row) {
                            #    
                            #    },
                            #    function($e) use ($self) {
                            #        $self->displayLogToStdOut($e);
                            #    
                            #    },
                            #    function() {
                            #    
                            #    }
                            #);
                            
                        }
                    }
                );
            }
        }
    }
    private function dequeueErrorMessage( $factory, $set, $score )
    {
        // Prevent `ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context`
        $client = $factory->createLazyClient($this->target);
        
        $deferred = new Deferred();
        $promise = $deferred->promise();
        
        $client->zrangebyscore($set, $score, $score)->then(function ($payload) use ($deferred, $set) {
            $payload_decoded = json_decode(is_array($payload) ? $payload[0] : "{}", true);
            $json_last_error = json_last_error();
            if ($json_last_error === JSON_ERROR_NONE) {
                echo $payload_decoded["filename"], " ";
                echo $payload_decoded["timestamp"], " ";
                echo $payload_decoded["message"], "\n";
            }
            $deferred->resolve($set);
        });
        $promise->then(function($set) use (&$client, $score) {
            $client->zremrangebyscore($set, $score, $score)->then(function() use ($client) {
                $client->close();
            });
        });
    }
    private function connectToRedis( $factory, $redisusr, $redispsw, $redishost, $redisport, $redistls, &$connected )
    {
        $client = $factory->createLazyClient($this->target = sprintf("redis" . ($redistls ? "s" : "") . "://%s:%s@%s:%d", $redisusr, $redispsw, $redishost, $redisport));
        $promise = $client->ping();
        $ret = Block\await($promise, $this->loop);
        $connected = $ret == "PONG";
        return $client;
    }
}
