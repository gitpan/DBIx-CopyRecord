#!/usr/bin/perl -w

use strict;
use Test::More;

unless (exists $ENV{'CR_MYSQL_DSN'}) {
  plan skip_all => "Set 'CR_MYSQL_DSN' environment variable to run this test";
}

unless (exists $ENV{'CR_MYSQL_USER'}) {
  plan skip_all => "Set 'CR_MYSQL_USER' environment variable to run this test";
}

unless (exists $ENV{'CR_MYSQL_PASS'}) {
  plan skip_all => "Set 'CR_MYSQL_PASS' environment variable to run this test";
}

my $DBD = 'mysql'; #DBD to test
                       
my $DB_USER = $ENV{'CR_MYSQL_USER'};
my $DB_PASS = $ENV{'CR_MYSQL_PASS'};
my $DB_DSN = $ENV{'CR_MYSQL_DSN'};

my @driver_names = DBI->available_drivers;

unless (grep { $_ eq $DBD } @driver_names) {
	plan skip_all => "Test irrelevant unless $DBD DBD is installed";
} else {
	plan tests => 16;
}

BEGIN { use_ok( 'DBI' ); }
require_ok( 'DBI' );

BEGIN { use_ok( 'CopyRecord' ); }
require_ok( 'CopyRecord' );

my $dbh=DBI->connect($DB_DSN,$DB_USER,	$DB_PASS,
    { 'RaiseError' => 0, 'AutoCommit' => 1 });
is(ref $dbh, 'DBI::db', 'Test the constructed object');

my $rv = $dbh->do( "DROP TABLE IF EXISTS xq9_invoice_master" );
if ($dbh->err) { warn "$DBI::err\n$DBI::errstr\nAborting..\n"; exit 0; }
is($rv, '0E0', 'DROP TABLE IF EXISTS xq9_invoice_master');

$rv = $dbh->do( "CREATE TABLE  xq9_invoice_master (
     invoice_number int(10) unsigned NOT NULL auto_increment,
     client_number int(10) unsigned NOT NULL,
     invoice_date timestamp NOT NULL default CURRENT_TIMESTAMP,
     billed char(1) NOT NULL default 'N',
     PRIMARY KEY  (invoice_number)
   ) ENGINE=MyISAM " );
if ($dbh->err) { warn "$DBI::err\n$DBI::errstr\nAborting..\n"; exit 0; }
is($rv, '0E0', 'CREATE TABLE xq9_invoice_master');

$rv = $dbh->do( "INSERT INTO xq9_invoice_master (invoice_number,client_number,invoice_date,billed) VALUES (1,7874,'2007-01-06 21:41:00','N')" );
if ($dbh->err) { warn "$DBI::err\n$DBI::errstr\nAborting..\n"; exit 0; }
is($rv, 1, 'INSERT INTO xq9_invoice_master record 1.');

$rv = $dbh->do( "INSERT INTO xq9_invoice_master (invoice_number,client_number,invoice_date,billed) VALUES (2,1216,'2007-01-06 21:41:00','N')" );
if ($dbh->err) { warn "$DBI::err\n$DBI::errstr\nAborting..\n"; exit 0; }
is($rv, 1, 'INSERT INTO xq9_invoice_master record 2.');

$rv = $dbh->do( "DROP TABLE IF EXISTS xq9_invoice_detail");
if ($dbh->err) { warn $dbh->{errormessage}."\nAborting..\n"; exit 0; }
is($rv, '0E0', 'DROP TABLE IF EXISTS xq9_invoice_detail');

$rv = $dbh->do( "CREATE TABLE xq9_invoice_detail (
     invoice_detail_id int(10) unsigned NOT NULL auto_increment,
     invoice_number int(10) unsigned NOT NULL,
     product_number int(10) unsigned NOT NULL,
     qry int(10) unsigned NOT NULL,
     price decimal(10,0) NOT NULL,
     ext_price decimal(10,0) NOT NULL,
     PRIMARY KEY  (invoice_detail_id),
     KEY Index_2 (invoice_number)
   ) ENGINE=MyISAM ");
if ($dbh->err) { warn "$DBI::err\n$DBI::errstr\nAborting..\n"; exit 0; }
is($rv, '0E0', 'CREATE TABLE xq9_invoice_detail');

$rv = $dbh->do( "INSERT INTO xq9_invoice_detail (invoice_detail_id,invoice_number,product_number,qry,price,ext_price) VALUES (1,1,12345,1,'10','10')" );
if ($dbh->err) { warn $dbh->{errormessage}."\nAborting..\n"; exit 0; }
is($rv, 1, 'INSERT INTO xq9_invoice_detail record 1.');

$rv = $dbh->do( "INSERT INTO xq9_invoice_detail (invoice_detail_id,invoice_number,product_number,qry,price,ext_price) VALUES (2,1,2322,3,'3','9')" );
if ($dbh->err) { warn "$DBI::err\n$DBI::errstr\nAborting..\n"; exit 0; }
is($rv, 1, 'INSERT INTO xq9_invoice_detail record 2.');

$rv = $dbh->do( "INSERT INTO xq9_invoice_detail (invoice_detail_id,invoice_number,product_number,qry,price,ext_price) VALUES (3,2,12345,2,'11','22')" );
if ($dbh->err) { warn "$DBI::err\n$DBI::errstr\nAborting..\n"; exit 0; }
is($rv, 1, 'INSERT INTO xq9_invoice_detail record 3.');

my $CR = CopyRecord->new ( $dbh );
isa_ok ($CR, 'CopyRecord');

$rv=$CR->copyrecord(
               table_name => 'xq9_invoice_master',
               primary_key => 'invoice_number',
               primary_key_value => 'NULL',
               where => 'invoice_number=2'  );
ok($rv gt 1, 'CR->copyrecords() failed.');


$rv = $dbh->do( "DROP TABLE IF EXISTS xq9_invoice_detail");
if ($dbh->err) { warn "$DBI::err\n$DBI::errstr\nAborting..\n"; exit 0; }
is($rv, '0E0', 'DROP TABLE IF EXISTS xq9_invoice_detail');

$rv = $dbh->do( "DROP TABLE IF EXISTS xq9_invoice_master");
if ($dbh->err) { warn "$DBI::err\n$DBI::errstr\nAborting..\n"; exit 0; }
is($rv, '0E0', 'DROP TABLE IF EXISTS xq9_invoice_master');

undef $dbh;
