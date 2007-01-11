
use Test::More tests => 2;

BEGIN { use_ok( 'CopyRecord' ); }

my $dbh;
my $object = CopyRecord->new ( \$dbh);
isa_ok ($object, 'CopyRecord');


