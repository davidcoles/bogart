#!/usr/bin/perl
use lib qw(.);
use strict;
use IPC::MM qw(mm_create mm_make_btree_table);
use File::Temp;
use Getopt::Std;
use bogart;
getopts('socn:u:', \my %opts) or die;

my $unix = defined $opts{u} ? $opts{u} : '/tmp/bogart.sock';
my $name = defined $opts{n} ? $opts{n} : 'default';

exit server() if $opts{s};

tie my %h, 'bogart::hash', $name, $unix or die "tie\n";

if(scalar @ARGV) {
    my $n = 1;
    %h = map { chop; $n++ => $_ } <>;
    exit;	
}

if($opts{o}) {
    foreach (sort { $a <=> $b } keys %h) { printf "%-4s: %s\n", $_, $h{$_} }
    exit;
}

while(my($k, $v) = each %h) { printf "%-4s: %s\n", $k, $v }




######################################################################
our $trafficker;
sub server {
    my $b;
    if($opts{c}) {
	$trafficker = new bogart::trafficker unless defined $trafficker;
	$b = new bogart::dealer(\&bogart_mule) or die;
    } else {
	$b = new bogart::dealer(\&ipc_mm_btree) or die;
    }
    $b->run($unix, 0777);
    exit;
}

sub bogart_mule { my $db = tie my %db, 'bogart::mule', $trafficker, @_ }

sub ipc_mm_btree {
    my($pkg, @args) = @_;
    my $MMSIZE = 0;
    my $MMFILE = tmpnam();
    my $mm = mm_create($MMSIZE, $MMFILE) or die;
    my $db = tie my %db, 'IPC::MM::BTree', mm_make_btree_table($mm);
}
