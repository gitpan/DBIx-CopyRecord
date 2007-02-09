package DBIx::CopyRecord;

use strict;
use DBI;

BEGIN {
    use Carp;
    use Exporter ();
    use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS $PACKAGE);
    $VERSION     = '0.008';
    @ISA         = qw(Exporter);
    @EXPORT      = qw();
    @EXPORT_OK   = qw();
    %EXPORT_TAGS = ();

    $Carp::CarpLevel = 1;
    $PACKAGE         = "DBIx::CopyRecord";

    #debug constant
    use constant DEBUG => 0;

}

sub new {
    my ( $class, @args ) = @_;

    my $self = bless( {}, ref($class) || $class );

    if ( !defined $args[0] ) {
        croak "$PACKAGE->new requires one value.  \$dbh\n";
    }
    $self->{_dbh} = $args[0];

    if (DEBUG) {
        select (STDOUT);
        $| = 1;
        use Data::Dumper;
    }

    return $self;
}

sub DESTROY () {
}

=head1 NAME

 DBIx::CopyRecord - module for copying record(s) in database within same table including all related ehild table(s); 

=head1 SYNOPSIS

 Perl module for copying record(s) in database within same table including all related ehild table(s); 

=head1 DESCRIPTION

 This module can copy record(s) in a database whild maintaining referential 
 integrity.  The C<copy> method is all that's needed.  It's useful for copying
 related record(s) and assigning a new key value to the new record(s).  All of
 this while maintaining referential integrity.  

 You can specify all of the relationships in the copy command, for example, if your DB is not using foreign keys.  Or, simply tell the method what the name of the foreign key is and the module will do the rest.

The copy method will return the assigned key value so that you can use it.

=head1 USAGE

  use DBIx::CopyRecord;                    
  my $CR = DBIx::CopyRecord->new( DB HANDLE );

  RV = $CR->copy(
        { table_name => TABLE NAME,
          primary_key => PRIMARY KEY COLUMN,
          primary_key_value => VALUE, NULL or SELECT,
          where => WHERE CONDITION,
          override => {
                        billed = 'N',
                        invoice_date = 'NULL'
                      }
          child => [ { table_name => CHILD TABLE NAME,
                       primary_key => CHILD PRIMARY KEY COLUMN,
                       primary_key_value => CHILD VALUE, NULL or SELECT, 
                       foreign_key => COLUMN NAME OF  },
                     { table_name => CHILD TABLE NAME } ] });


Child table_name entry without additional arguments will attempt to figure out
the primary key and foreign key from the database.  



=cut

sub copy() {

    my $self = shift;
    my ($args) = @_;
    my ( $key_value, $child, $parent, $children );

    $parent   = $$args{parent};
    $children = $$args{child};

    #$parent=get_real_values($parent);
    #$children=get_real_values($children);

    if (DEBUG) {
        print "Copy->Parent: \n";
        print Dumper($parent);
        print "Copy->Child: \n";
        foreach my $x (@$children){
          $x=$self->get_real_values($x);
          print Dumper($x);
        }
    }

    #$self->check_required_fields( 'parent', $parent );
    if ( $parent ) {
      $key_value = $self->_copy($parent);
    }

    if ($children) {
        foreach $child (@$children) {
            if (DEBUG) {
                print "child: \n";
                print Dumper($child);
            }
    #        $self->check_required_fields( 'child', $child );

            if ( ! defined $$child{where} ) {
              $$child{where} = $$parent{where};
            }

            if ( ! defined $$child{foreign_key_value} ) {
              $$child{foreign_key_value} = $key_value;
            }

            $self->_copy($child);
        }
    }

    return $key_value;
}

#
# Actual work is done here.
#
sub _copy() {
    my $self = shift;
    my ($args) = @_;
    my ( @field_name_list, @field_value_list );
    my (
        $select_query_sql, $select_queryh, $insert_query_sql,
        $insert_queryh,    $field_part,    $value_part,
        $record_hashref,   $field_name,    $field_value
    );

    if (DEBUG) {
        print "_copy: \n";
        print Dumper($args);
    }

# Select all columns from source table
    $select_query_sql = qq(
                        SELECT * 
                          FROM $$args{table_name} 
                            WHERE $$args{where} );

    print STDERR "\n$select_query_sql\n" if DEBUG;

    $select_queryh = $self->{_dbh}->prepare($select_query_sql);
    $select_queryh->execute();

### Loop through all matching records
    while ( $record_hashref = $select_queryh->fetchrow_hashref ) {

### Initialize 
        $field_part       = '';
        $value_part       = '';
        $insert_query_sql = '';
        @field_name_list  = ();
        @field_value_list = ();

### Override what needs to be
        if ( $$args{override} ) {
            my $override = $$args{override};
            $override = $self->get_real_values($override);

            foreach ( keys %$override ) {
                print STDERR
"Reassigning: $_ from $$record_hashref{$_} to $$override{$_}\n"
                  if DEBUG;
                if ( $$override{$_} ne 'NULL' ) {
                    $$record_hashref{$_} = $$override{$_};
                }
                else {
                    delete $$record_hashref{$_};
                }

            }
        }

### Process foreign key
        if ( $$args{foreign_key_value} ) {
            my $foreign_key_value = $$args{foreign_key_value};
            $foreign_key_value = $self->get_real_values($foreign_key_value);

            $$record_hashref{$$args{foreign_key}} = $$args{foreign_key_value};

        }

### Get CHAR field names
        my $sth =
          $self->{_dbh}->column_info( undef, undef, $$args{table_name}, "%" );
        my $cnames = $sth->fetchall_hashref("COLUMN_NAME");

        while ( ( $field_name, $field_value ) = each %$record_hashref ) {

            if (   $field_name ne $$args{primary_key}
                || $$args{primary_key_value} ne 'NULL' )
            {
                if ( $$cnames{$field_name}{TYPE_NAME} =~ /[CHAR|DATE|TIME]/ ) {
                    $field_value =
                      qq('$field_value');    ### Enclose CHAR fields in quotes
                }
                push( @field_name_list,  $field_name );
                push( @field_value_list, $field_value );
            }
        }

        $field_part = join( ', ', @field_name_list );
        $value_part = join( ', ', @field_value_list );

### insert new record
        $insert_query_sql = qq( 
        INSERT INTO $$args{table_name} ( $field_part ) VALUES ( $value_part ) );

        print STDERR "$insert_query_sql\n" if DEBUG;

        $insert_queryh = $self->{_dbh}->prepare($insert_query_sql);
        $insert_queryh->execute();
    }

### Assuming Mysql
    my $select_idh = $self->{_dbh}->prepare("SELECT LAST_INSERT_ID()");
    $select_idh->execute();
    my $assigned_id = $select_idh->fetchrow();

    return $assigned_id;

}

sub check_required_fields {
    my $self = shift;
    my ( $list, $args ) = @_;
    my %required_fields_list = (
        parent => [ 'table_name', 'primary_key', 'where' ],
        child  => ['table_name']
    );

    my @required_fields = $required_fields_list{$list};
    foreach (@required_fields) {
        if ( !defined $$args{$_} ) {
            croak "$PACKAGE: $_ is required.\n";
        }
    }
}

sub get_real_values {
    my $self = shift;
    my $args = shift;

    if ( not ref $args ) {
        $args;
    }
    elsif ( ref $args eq "ARRAY" ) {
        [ map get_real_values($_), @$args ];
    }
    elsif ( ref $args eq "HASH" ) {
        +{ map { $_ => $self->get_real_values( $args->{$_} ) } keys %$args };
    }
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

