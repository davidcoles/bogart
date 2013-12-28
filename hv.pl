#!/usr/bin/perl
use lib qw(. cpg);
package hv;
#use Corosync::Quorum qw /:constants/;
use sharer;
use strict;
use Switch;
use base qw(sharer);

our $Q :shared = 0;
our $q = undef;
our $s;
 
sub new       { shift->SUPER::new('hv') }
sub claim     { shift->send_query('claim', @_) }

sub release   { shift->send_query('release', @_) }
sub unclaimed { shift->send_query('unclaimed', @_) }
sub spare     { shift->send_query('spare', @_) }
sub mine      { shift->send_query('mine', @_) }
sub add       { shift->send_query('add', @_) }

sub vms   { defined $_[0]->shared->{vms}   ? %{$_[0]->shared->{vms}}   : () }
sub nodes { defined $_[0]->shared->{nodes} ? %{$_[0]->shared->{nodes}} : () }

sub notify {
    if(!$ENV{DEBUG} && !$_[0]) {
	warn "Lost quorum!\n";
	map { $_->kill('KILL') } threads->list(threads::running);
	die;
    }
    $Q = $_[0];
} 

sub confchg {
    my($self, $group, $members, $leaving, $joining) = @_;
    $s = $self;
#    $q = new Corosync::Quorum(callbacks => { notification => \&notify })
#	unless defined $q;
#    $q->dispatch(CS_DISPATCH_ALL) if defined $q;

    my $shared = $self->{_shd};
    my $nodes = $shared->{nodes};
    my $vms = $shared->{vms};
    
    foreach my $n (@$leaving) {
	delete $nodes->{$n};
	map { $vms->{$_} = undef if $n eq $vms->{$_} } (keys %$vms);
    }

    $self->display if scalar(@$leaving);
}

sub event {
    my($self, $from, $op, @args) = @_;
#    $q->dispatch(CS_DISPATCH_ALL);
    switch($op) {
	case 'claim' {
	    my $vm = $args[0];
	    my $cap = $args[1];
	    $self->shared->{nodes}->{$from} = $cap;
	    return unless defined $vm;
	    return if defined $self->shared->{vms}->{$vm};
	    $self->shared->{vms}->{$vm} = $from;
	}
	case 'release' {
	    my $vm = $args[0];
	    return unless defined $vm;
	    $self->shared->{vms}->{$vm} = undef
		if $self->shared->{vms}->{$vm} eq $from;
	}
	case 'add'     {
	    my $vm = $args[0];
	    return unless defined $vm;
	    $self->shared->{vms}->{$vm} = undef
		unless exists $self->shared->{vms}->{$vm};
	}
    }
    $self->display;
}

sub query {
    my($self, $op, @args) = @_;
#    $q->dispatch(CS_DISPATCH_ALL);
    switch($op) {
	case 'claim' { $self->sendpdu(['claim', @args]) }
	case 'add'   { $self->sendpdu(['add', @args]) }
	case 'release' { $self->sendpdu(['release', @args]) }
	case 'unclaimed' { return [ $self->_unclaimed] }
	case 'spare'     { return [ $self->_spare ]  }
	case 'mine'      { return [ $self->_mine ]  }
    }
    return undef;
}

sub _spare {
    my($self) = @_;
    my %v = $self->vms;
    my %n = $self->nodes;
    my %ok;
    
    while(my($n, $cap) = each %n) {
	my $count = scalar( grep { $v{$_} eq $n } keys %v);
	$ok{$n} = $count < $cap ? $cap - $count : 0;
    }
    
    return ($ok{$self->nodeid}, %ok);
}

sub _unclaimed {
    my($self) = @_;
    my %v = $self->vms;
    return sort grep { !defined $v{$_} } keys %v;
}

sub _mine {
    my($self) = @_;
    my $i = $self->nodeid;
    my %v = $self->vms;
    sort grep { $v{$_} eq $i } keys %v;
}


sub display { 
    my($self) = @_;
    my $id = $self->nodeid;
    my %nodes = $self->nodes;
    my %vms = $self->vms;
    my @unc = $self->_unclaimed;
    my $date = localtime();

    printf "%d %s %s %d\n", $Q, $date, $self->md5, scalar(@unc);
    foreach my $n (sort keys %nodes) {
	my @vms = sort grep { $vms{$_} eq $n } keys %vms;
	printf "%s%-20s %s\n", $n eq $id ? '>':' ',  $n, join(',', @vms);
    }
    printf "%s%-20s %s\n\n", ' ',  ' ', join(',',  @unc);
}

package main;
my $cap = 7;
my $app = new hv();

if(defined(my $add = shift)) {
  add:
    $app->add($add);
    $add = shift;
    goto add if defined $add;
    exit;
} 

$app->claim(undef, $cap);

while(1) {
    my @vms = $app->unclaimed;
    my @mine = $app->mine;
    my($spare, %spare) = $app->spare;
    my $most = 0;
    my $best = 1;

    map { $most = $_ if $_ > $most } values %spare;
    map { $best = 0  if $_ > $spare} values %spare;

    $app->claim(shift @vms, $cap) if scalar @vms && $spare && $best;
    $app->release(pop @mine) if scalar @mine && !scalar @vms &&
	$most > $spare+1 && !int rand(3);
    sleep 1;
}


__END__;

=head1 NAME

hv.pl - a simulation of hypervisors co-operating to run virtual machines

=head1 SYNOPSIS

Run multiple copies (4, say) of hv.pl in diffent terminals (needs to be done as
root to be able to communicate with the Corosync daemon), eg.:

  sudo ./hv.pl

With these multiple copies running add a list of tokens representing
VMs to the cluster:

  sudo ./hv.pl 0 1 2 3 4 5 6 7 8 9 a b c d e f

Note how each instance of hv.pl claims a number of VMs (up to 7). Each
hv.pl instance displays the state of all others, it's own status
indicated by the line prefixed with a ">" symbol. If there is not
enough resource to deal with all VMs in the cluster they sit on a line
by themselves without a nodeid prefixing them

Kill one instance of hv.pl and watch the now orphaned resources get
redstibuted to the remaining instances. Restart the instance and the
resources will be gradually redistributed to give a balanced number of
resources on each instance.

If you have set up Corosync to talk on your LAN (update "bindnetaddr"
to your LAN IP address in /etc/corosync/corosync.conf) then you can
run hv.pl on multiple machines and they will all co-operate in the
same way that they would on a single machine.

=head1 DESCRIPTION

Just a bit of fun. The algorithm and code is pretty whack. The MD5 sum
in the line just above the resource spread is a running digest of all
the update messages that have gone into making up the current state
and so should be the same on all instances. When a new node joins it
requests a state transfer which includes the current MD5 revision -
from then on it calculates its own digest independently accoring to
subsequent updates.

Uses the sharer.pm (and by extension state.pm) module to deal with
handling updates from the network and decisions/requests made by the
local application.

=head1 DEPENDENCIES

=over

=item perl-Corosync-CPG - Perl interface to the Corosync CPG client library

See https://github.com/cventers/perl-Corosync-CPG.git

=item Corosync - The Corosync Cluster Engine.

Corosync provides clustering infracture such as membership, messaging
and quorum. See your 

=back

=head1 AUTHOR

David Coles <david.coles@potentialdeathtrap.net>

=head1 COPYRIGHT

Copyright (c) 2013 David Coles. All rights reserved. This program is free
software; you can redistribute it and/or modify it under the same terms as Perl
itself.

=head1 URL

