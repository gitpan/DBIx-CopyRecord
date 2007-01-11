package CopyRecord;

use strict;
use DBI;

BEGIN {
    use Carp;
    use Exporter ();
    use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS $PACKAGE);
    $VERSION     = '0.001';
    @ISA         = qw(Exporter);
    @EXPORT      = qw();
    @EXPORT_OK   = qw();
    %EXPORT_TAGS = ();

    $Carp::CarpLevel = 1;
    $PACKAGE = "CopyRecord";

    #debug constant
    use constant DEBUG => 0;

}


sub new
{
    my ($class, @args) = @_;

    my $self = bless ({}, ref ($class) || $class);

    if ( !defined $args[0] ){
      croak "$PACKAGE->new requires one value.  \$dbh\n";
    }
    $self->{_dbh} = $args[0];

    return $self;
}

sub DESTROY () {
}

=head1 NAME

DBIx::CopyRecord - Perl module for copying records in databases within same table including all child tables;

=head1 SYNOPSIS

  use DBIx::CopyRecord;

  # connect 
  my $dbh = DBI->connect('dbi:MySQL:','login','password');
  my $CR = DBIx::CopyRecord->new($dbh);

### child table_name only is not yet supported.

  $rv=$CR->copyrecord(
                      { table_name => 'invoice',
                        primary_key => 'invoice_number',
                        primary_key_value => 'select seq_invoice.nextvalue FROM dual',
                        where => 'invoice_number=100',
                        child => [ { table_name => products,
                                     primary_key => line_item_id,
                                     primary_key_value => 'select seq_invoice_detail.nextvalue FROM dual', 
                                     foreign_key => invoice_number },
                                   { table_name => services } ] });

	# disconnect database
	$dbh->disconnect;


  NOTE:primary_key_value = 'NULL' for MySQL if auto_increment, 
                           a value or 'select value from sequence'


=head1 DESCRIPTION

This module can copy records in a database whild maintaining referential 
integrity.  The C<copy> method is all that's needed.

=head1 USAGE

  use DBIx::CopyRecord;                    
  my $CR = DBIx::CopyRecord->new( DB HANDLE );

  RV = $CR->copyrecord(
        { table_name => TABLE NAME,
          primary_key => PRIMARY KEY COLUMN,
          primary_key_value => VALUE, NULL or SELECT,
          where => WHERE CONDITION,
          child => [ { table_name => CHILD TABLE NAME,
                       primary_key => CHILD PRIMARY KEY COLUMN,
                       primary_key_value => CHILD VALUE, NULL or SELECT, 
                       foreign_key => COLUMN NAME OF  },
                     { table_name => CHILD TABLE NAME } ] });

Argument list:
  table_name
  primary_key
  primary_key_value
  where
  child

Child table_name entry without additional arguments will attempt to figure out
the primary key and foreign key from the database.  



=cut

sub copyrecord() {

	my $self = shift;
  my %args = @_;
  my ($key_value, @required_fields, $child_hash) ;
  
  my $cnt=1;
  while ( my ($l_key, $l_value) = each %args){
    #warn "$PACKAGE: $cnt $l_key = $l_value\n";
    $cnt++;
  }
  @required_fields = ( 'table_name', 
                       'primary_key', 
                       'where');

  foreach (@required_fields){
    if ( !defined $args{$_} ) {
	    croak "$PACKAGE $_ is required.\n";
	  }
	}

  $key_value=$self->copy( %args );

  if ( $args{child} ){
    foreach $child_hash ($args{child}){
      &copy($child_hash);     
		}
	}

  return $key_value;
}

sub copy(){
	my $self = shift;
  my %args = @_;
  my ($select_query_sql, $select_queryh, 
      $insert_query_sql, $insert_queryh,
      $field_part, $value_part,
      $record_hashref,
      $field_name,
      $field_value);
  my (@field_name_list, @field_value_list);

  my $cnt=1;
  while ( my ($l_key, $l_value) = each %args){
    print STDERR "\nCOPY: $cnt.  $l_key = $l_value" if DEBUG;
    $cnt++;
  }

  $select_query_sql = qq(
                        SELECT * 
                          FROM $args{table_name} 
                            WHERE $args{where} );

  print STDERR "\n$select_query_sql\n" if DEBUG;
	
  $select_queryh=$self->{_dbh}->prepare($select_query_sql);
  $select_queryh->execute();

### Loop through all matching records
  while ($record_hashref = $select_queryh->fetchrow_hashref) {

    ### Get CHAR field names
	  my $sth = $self->{_dbh}->column_info( undef, undef, $args{table_name}, "%");
	  my $cnames= $sth->fetchall_hashref( "COLUMN_NAME" ); 

    while (( $field_name, $field_value) = each %$record_hashref) {

      if ( $field_name eq $args{primary_key} && 
           $args{primary_key_value} eq 'NULL'  ){
        #push ( @field_name_list, $field_name);
        #push ( @field_value_list, $args{primary_key_value});
      } else {
        if ( $$cnames{$field_name}{TYPE_NAME} =~ /[CHAR|DATE|TIME]/ ){  
          $field_value = qq('$field_value');  ### Enclose CHAR fields in quotes
        }
        push ( @field_name_list, $field_name);
        push ( @field_value_list, $field_value);
      }
    }

    $field_part = join(', ', @field_name_list);
    $value_part = join(', ', @field_value_list);
	
    $insert_query_sql = qq( 
        INSERT INTO $args{table_name} ( $field_part ) VALUES ( $value_part ) );

    print STDERR "$insert_query_sql\n" if DEBUG;
	
    $insert_queryh=$self->{_dbh}->prepare($insert_query_sql);
    $insert_queryh->execute();
  }

### Assuming Mysql
  my $select_idh=$self->{_dbh}->prepare("SELECT LAST_INSERT_ID()");
  $select_idh->execute();
  my $assigned_id=$select_idh->fetchrow();

  return $assigned_id;  
  
}


=head1 AUTHOR

    Jack Bilemjian <jck000@gmail.com>

=head1 COPYRIGHT

This program is free software; you can redistribute
it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the
LICENSE file included with this module.


=head1 SEE ALSO

DBI(1).

=cut

1;

