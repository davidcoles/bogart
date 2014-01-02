#!/usr/bin/perl
use lib qw(.);
#use strict;
use IPC::MM qw(mm_create mm_make_btree_table);
use File::Temp;
use Getopt::Std;
use bogart;
getopts('socpn:u:', \my %opts) or die;

my $unix = defined $opts{u} ? $opts{u} : '/tmp/bogart.sock';
my $name = defined $opts{n} ? $opts{n} : 'default';
my $traf;
my %hash;

exit server() if $opts{s};

if($opts{p}) {
    tie %hash, 'bogart::peer', $name or die "tie\n";
} else {
    tie %hash, 'bogart::hash', $unix, $name or die "tie\n";
}

if(scalar @ARGV) {
    %hash = ();
    while(<>) { chop; $hash{$.} = ($_) }
} elsif($opts{o}) {
    foreach (sort {$a<=>$b} keys %hash) { printf "%-4s: %s\n", $_, $hash{$_} }
} else {
    while(my($k, $v) = each %hash) { printf "%-4s: %s\n", $k, $v }
}

untie %hash;


######################################################################
sub server {
    my $deal;
    if($opts{c}) {
	$traf = new bogart::trafficker() unless defined $traf;
	$deal = new bogart::dealer(\&bogart_mule) or die;
    } else {
	$deal = new bogart::dealer(\&ipc_mm_btree) or die;
    }
    $deal->run($unix, 0777);
    exit;
}

sub bogart_mule { my $db = tie my %db, 'bogart::mule', $traf, @_ }

sub ipc_mm_btree {
    my($pkg, @args) = @_;
    my $MMSIZE = 0;
    my $MMFILE = tmpnam();
    my $mm = mm_create($MMSIZE, $MMFILE) or die;
    my $db = tie my %db, 'IPC::MM::BTree', mm_make_btree_table($mm);
}










__END__;

=head1 NAME

bogart,pl - sharing hashes over the network

=head1 SYNOPSIS

Start a server in one terminal:

    ./bogart.pl -s

In another terminal put data into the hash:

    ./bogart.pl /etc/hosts

When this has finished retrieve data from the hash:

    ./bogart.pl
    1   : 127.0.0.1 localhost
    2   : 127.0.1.1 che.localnet che
    3   : 
    4   : # The following lines are desirable for IPv6 capable hosts
    5   : ::1     ip6-localhost ip6-loopback
    6   : fe00::0 ip6-localnet
    7   : ff00::0 ip6-mcastprefix
    8   : ff02::1 ip6-allnodes
    9   : ff02::2 ip6-allrouters

All restricted to one node. Boring.

Start a Corosync enabled server (need to be root):

    sudo ./bogart.pl -s -c

In another terminal populate it:

    ./bogart.pl /etc/hosts

Dump data out:

    ./bogart.pl
    1   : 127.0.0.1 localhost
    2   : 127.0.1.1 che.localnet che
    ...
    
In a third terminal start a new server:

    sudo ./bogart.pl -s -c

Data is quietly replicated. Kill the original server (Ctrl-C). Dump data:

    ./bogart.pl
    1   : 127.0.0.1localhost
    2   : 127.0.1.1che.localnetche
    ...

The data was not lost! Restart the fiest one, kill the seond one -
data still there! Make sure Corosync is set up to work on your LAN,
run servers on multiple nodes - data is replicated amongst all nodes
until the last remaining server is killed off.

You will need a local running server which is accepting connections on
the UNIX domain socket to dump the data out of course, but it's
entirely possible to combine the classes into one process which
replicates the data to itself so no socket is needed, but for the
purposes of this demonstration it's not so effective as they would be
short lived procesess and not around long enough to keep data
replicated.

=head1 DESCRIPTION

The bogart::dealer class implements a simple threaded server which
allows multiple client processes to access hashes via a UNIX domain socket.

A new instance is created with a callback for creating new hashes (of
arbitrary class) then the C<run()> method starts the server listening on a
socket.

Hashes can be accessed with the tieing the bogart::hash class with a
hash name (multiple hases are supported on one server) and the UNIX
socket address for communicating with the server, eg.:

 tie my %h, 'bogart::hash', 'MyHash', '/tmp/MyHash.sock';

Client processes can then read/write from the hashes and see each
other entries.

The bogart::trafficker class allows hashes to be shared over the
network with Corosync's closed process group implementation and a
shared state protocol.

A glue class (bogart::mule) is used to link bogart::dealer with
bogart::trafficker and transparently replicates the hashes across all
participating nodes on the network.


=head1 DEPENDENCIES

=over

=item perl-Corosync-CPG - Perl interface to the Corosync CPG client library

See https://github.com/cventers/perl-Corosync-CPG.git

=item Corosync - The Corosync Cluster Engine.

Corosync provides clustering infracture such as membership, messaging
and quorum. See your 

=item IPC::MM - Perl interface to the MM library

Seems to the the only tied hash interface I've found which doesn't
cause problems with concurrent access to the hash.

=item JSON, MIME::Base64 sundry others for serialisation, etc.

=back

=head1 AUTHOR

David Coles <david.coles@potentialdeathtrap.net>

=head1 COPYRIGHT

Copyright (c) 2013 David Coles. All rights reserved. This program is free
software; you can redistribute it and/or modify it under the same terms as Perl
itself.

=head1 URL
