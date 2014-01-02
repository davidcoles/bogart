#!/usr/bin/perl

package bogart::dealer;
use strict;
use Switch;
use JSON;
use IO::Socket::UNIX;
use threads;
use Thread::Queue;

sub new {
    my($pkg, $tiehash) = @_;
    my %hash;
    bless { _tieh => $tiehash,_hash => \%hash }, $pkg;
}

sub TIEHASH {
    my($self, $hash, @args) = @_;
    return 1 if exists $self->{_hash}{$hash};
    my $tiehash = $self->{_tieh};
    my $h = &{$tiehash}($hash, @args);
    return 0 unless defined $h;
    $self->{_hash}{$hash} = $h;
    return 1;
}

sub run {
    my($self, $path, $mode) = @_;
    $SIG{PIPE} = sub { };
    unlink($path);
    
    my $l = IO::Socket::UNIX->new(Local => $path, Listen => 5) or die "$!\n";
    my $q = new Thread::Queue;
    my $t = threads->create(\&server, $l, $q);
    chmod($mode, $path) if defined $mode;
    
    while(defined(my $m = $q->dequeue())) {
	my($recv, $hash, $oper, @args) = @$m;
	my $r = [undef];
	
	switch($oper) {
	    case 'TIEHASH'  { $r = [$self->TIEHASH($hash, @args) ] }
	    case 'CLEAR'    { $r = [$self->{_hash}{$hash}->CLEAR   (@args)] }
	    case 'DELETE'   { $r = [$self->{_hash}{$hash}->DELETE  (@args)] }
	    case 'STORE'    { $r = [$self->{_hash}{$hash}->STORE   (@args)] }
	    case 'FETCH'    { $r = [$self->{_hash}{$hash}->FETCH   (@args)] }
	    case 'EXISTS'   { $r = [$self->{_hash}{$hash}->EXISTS  (@args)] }
	    case 'FIRSTKEY' { $r = [$self->{_hash}{$hash}->FIRSTKEY(@args)] }
	    case 'NEXTKEY'  { $r = [$self->{_hash}{$hash}->NEXTKEY (@args)] }
	}
	$recv->enqueue($r);
    }
}

sub server {
    my($l, $sendq) = @_;
    while(defined(my $s = $l->accept)) {
	threads->create(
	    sub {
		my($s, $send) = @_;
		my $recv = Thread::Queue->new;
		my $json = JSON->new->indent(0)->utf8(1);
		while(<$s>) {
		    my $o = from_json($_);
		    my($hash, $oper, @args) = @$o;
		    $send->enqueue([$recv, $hash, $oper, @args]);
		    printf $s "%s\n", $json->encode($recv->dequeue);
		}
		close($s);
		return 0;
	    }, $s,$sendq);
	close($s);
	#map { $_->join } threads->list(threads::joinable); # bug?
    }
}


package bogart::hash;
use IO::Socket::UNIX;
use AutoLoader 'AUTOLOAD';
use JSON;
our $AUTOLOAD;

sub AUTOLOAD {
    my($self, @args) = @_;
    my $sub = $AUTOLOAD;
    (my $constname = $sub) =~ s/.*:://;
    my $s = $self->{_s};
    printf $s "%s\n", $self->{_j}->encode([ $self->{_h}, $constname, @args ]);
    my $a = <$s>;
    defined $a && $a ne '' ? @{from_json($a)}[0] : undef;    
}

sub TIEHASH {
    my($pkg, $p, $h, @args) = @_;
    my $j = JSON->new->indent(0)->utf8(1);
    my $s = IO::Socket::UNIX->new(Peer => $p) or return undef;
    printf $s "%s\n", $j->encode([ $h, 'TIEHASH',  @args ]);
    my $a = <$s>;
    return defined $a && $a ne '' && @{from_json($a)}[0] ?
	bless { _j => $j,_s => $s, _h => $h }, $pkg : undef;
}

sub DESTROY {
    my($self) = @_;
    close($self->{_s});
}











######################################################################
















package bogart::trafficker;
use sharer;
use strict;
use Switch;
use base qw(sharer);
use JSON;
use MIME::Base64;
use IPC::MM qw(mm_create mm_make_btree_table);
use File::Temp;

our $JSON = JSON->new->indent(0)->utf8(1);

sub serialise {
    encode_base64(to_json(shift->{_shd}));
}
sub deserialise {
    $_[0]->initialise;
    $_[0]->{_shd} = {};
    $_[0]->{_db} = {};
    my %d = %{from_json(decode_base64($_[1]))};
    foreach(keys %d) {
	warn "deserialised $_ ...\n" if $ENV{DEBUG};
	my $val = $d{$_};
	my $MMSIZE = 0;
	my $MMFILE = tmpnam();
	my $mm = mm_create($MMSIZE, $MMFILE) or die;
	my $db = tie my %db, 'IPC::MM::BTree', mm_make_btree_table($mm);
	$_[0]->{_shd}->{$_} = \%db;
	$_[0]->{_db}->{$_} = $db;
	%db = %$val;
    }
}
sub initialise {
    $_[0]->{_shd} = {};
    $_[0]->{_db} = {};
}
sub dump {
    shift->serialise;
}

sub tiehash {
    my($self, $d) = @_;
    return if exists $self->{_db} && exists $self->{_db}->{$d};
    #warn "=== $d\n";
    my $MMSIZE = 0;
    my $MMFILE = tmpnam();
    my $mm = mm_create($MMSIZE, $MMFILE) or die;
    my $db = tie my %db, 'IPC::MM::BTree', mm_make_btree_table($mm);
    $self->{_shd}->{$d} = \%db;
    $self->{_db}->{$d} = $db;
}

sub event {
    my($self, $from, $op, @args) = @_;
    my $db = shift @args;
    switch($op) {
	case 'TIEHASH' { $self->tiehash($db) }
	case 'CLEAR'   { $self->{_db}->{$db}->CLEAR }
	case 'DELETE'  { $self->{_db}->{$db}->DELETE(@args) }
	case 'STORE'   { $self->{_db}->{$db}->STORE (@args) }
    }
}

sub query {
    my($self, $op, @args) = @_;
    my $db = shift @args;
    my $r = [];
    return [1] if $op eq 'TIEHASH' && exists $self->{_db}->{$db};
    return $r if $op ne 'TIEHASH' && !exists $self->{_db}->{$db};
    switch($op) {
	case 'TIEHASH'  { $r = [$self->send_event([$op, $db, @args])] }
	case 'CLEAR'    { $r = [$self->send_event([$op, $db, @args])] }
	case 'DELETE'   { $r = [$self->send_event([$op, $db, @args])] }
	case 'STORE'    { $r = [$self->send_event([$op, $db, @args])] }
	case 'FETCH'    { $r = [$self->{_db}->{$db}->FETCH   (@args)] }
	case 'EXISTS'   { $r = [$self->{_db}->{$db}->EXISTS  (@args)] }
	case 'FIRSTKEY' { $r = [$self->{_db}->{$db}->FIRSTKEY(@args)] }
	case 'NEXTKEY'  { $r = [$self->{_db}->{$db}->NEXTKEY (@args)] }
    }
    return $r;
}

sub TIEHASH  { shift->send_query('TIEHASH',  @_) }
sub CLEAR    { shift->send_query('CLEAR',    @_) }
sub DELETE   { shift->send_query('DELETE',   @_) }
sub STORE    { shift->send_query('STORE',    @_) }
sub FETCH    { shift->send_query('FETCH',    @_) }
sub EXISTS   { shift->send_query('EXISTS',   @_) }
sub FIRSTKEY { shift->send_query('FIRSTKEY', @_) }
sub NEXTKEY  { shift->send_query('NEXTKEY',  @_) }







package bogart::mule;
sub TIEHASH {
    my($pkg, $traf, $name, @args) = @_;
    $traf->TIEHASH($name);
    bless { _name => $name, _traf => $traf }, $pkg;
}

sub CLEAR    { $_[0]->{_traf}->CLEAR   (shift->{_name}, @_) }
sub DELETE   { $_[0]->{_traf}->DELETE  (shift->{_name}, @_) }
sub STORE    { $_[0]->{_traf}->STORE   (shift->{_name}, @_) }
sub FETCH    { $_[0]->{_traf}->FETCH   (shift->{_name}, @_) }
sub EXISTS   { $_[0]->{_traf}->EXISTS  (shift->{_name}, @_) }
sub FIRSTKEY { $_[0]->{_traf}->FIRSTKEY(shift->{_name}, @_) }
sub NEXTKEY  { $_[0]->{_traf}->NEXTKEY (shift->{_name}, @_) }


package bogart::peer;
use base qw(bogart::mule);
our @traf;
sub TIEHASH {
    push(@traf, scalar(@traf) ? $traf[0] : new bogart::trafficker);
    shift->SUPER::TIEHASH($traf[0], @_)
}

sub DESTROY {
    $traf[0]->done if scalar(@traf) == 1;
    pop @traf;
}






















































































1;
__END__;

# was a single hash instance version
package bogart::dealer;
use sharer;
use strict;
use Switch;
use base qw(sharer);
use JSON;
use MIME::Base64;
use IPC::MM qw(mm_create mm_make_btree_table);

sub new { shift->SUPER::new('bogart::dealer_'.$_[0]) }

sub serialise { encode_base64(to_json(shift->{_shd})) }
sub deserialise {
    $_[0]->initialise;
    %{$_[0]->{_shd}} = %{from_json(decode_base64($_[1]))};
}
sub initialise {
    my($self, %db) = @_;
    my $MMSIZE = 0;
    my $MMFILE = '';
    my $mm = mm_create($MMSIZE, $MMFILE) or die;
    my $db = tie my %db, 'IPC::MM::BTree', mm_make_btree_table($mm);
    $_[0]->{_shd} = \%db;
    $_[0]->{_db} = $db;
}
sub dump { shift->serialise }

sub event {
    my($self, $from, $op, @args) = @_;
    switch($op) {
	case 'CLEAR'  { $self->{_db}->CLEAR }
	case 'DELETE' { $self->{_db}->DELETE(@args) }
	case 'STORE'  { $self->{_db}->STORE (@args) }
    }
}

sub query {
    my($self, $op, @args) = @_;
    my $r = [];
    switch($op) {
	case 'CLEAR'    { $r = [$self->send_event([$op, @args])] }
	case 'DELETE'   { $r = [$self->send_event([$op, @args])] }
	case 'STORE'    { $r = [$self->send_event([$op, @args])] }
	case 'FETCH'    { $r = [$self->{_db}->FETCH   (@args)] }
	case 'EXISTS'   { $r = [$self->{_db}->EXISTS  (@args)] }
	case 'FIRSTKEY' { $r = [$self->{_db}->FIRSTKEY(@args)] }
	case 'NEXTKEY'  { $r = [$self->{_db}->NEXTKEY (@args)] }
    }
    return $r;
}

sub TIEHASH  { shift->new(@_) }
sub CLEAR    { shift->send_query('CLEAR',    @_) }
sub DELETE   { shift->send_query('DELETE',   @_) }
sub STORE    { shift->send_query('STORE',    @_) }
sub FETCH    { shift->send_query('FETCH',    @_) }
sub EXISTS   { shift->send_query('EXISTS',   @_) }
sub FIRSTKEY { shift->send_query('FIRSTKEY', @_) }
sub NEXTKEY  { shift->send_query('NEXTKEY',  @_) }


















