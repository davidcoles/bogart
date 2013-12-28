#!/usr/bin/perl
use lib qw(. cpg);
use state;

package sharer;
use strict;
use Switch;
use JSON;
use MIME::Base64;
use threads;
use base qw(state);

sub serialise     { encode_base64(to_json($_[0]->shared)) }
sub deserialise   { $_[0]->{_shd} = from_json(decode_base64($_[1])) }
sub initialise    { $_[0]->{_shd} = { _date => localtime()."" } }
sub dump          { to_json(defined $_[0]->shared ? $_[0]->shared : {} ) }

#sub initialise    { }
#sub deserialise   { }
#sub serialise     { "" }
#sub dump          { "" }

sub process_event { 
    my($self, $event, $from) = @_;
    my(@args) = @$event;
    $self->event($from, @args);
}

sub new {
    my($pkg, $group) = @_;
    my $c_r = new IO::Handle;
    my $c_w = new IO::Handle;
    my $s_r = new IO::Handle;
    my $s_w = new IO::Handle;
    
    pipe($c_r, $c_w);
    pipe($s_r, $s_w);

    select((select($c_r), $|=1)[0]);
    select((select($s_w), $|=1)[0]);
    
    my $j = JSON->new->indent(0)->utf8(1);
    my $self = bless { _r => $c_r, _w => $s_w, _json=>$j }, $pkg;
    
    $self->{_cpg} = threads->create(sub {
	$pkg->backend($group, undef, $s_r, $c_w, $c_r, $s_w)});
    
    close($s_r);
    close($c_w);
    $self;
}

sub DESTROY {
    my($self) = @_;
    close($self->_w);
    close($self->_r);
    $self->{_cpg}->join;
}

sub shared { $_[0]->{_shd} }
sub json   { $_[0]->{_json} }
sub send_query {
    my($self, @args) = @_;
    my $w = $self->_w;
    my $r = $self->_r;
    return unless defined $w;
    print $w sprintf "%s\n", $self->json->encode(\@args);
    my $a = <$r>;
    my @a = defined $a && $a ne '' ? @{from_json($a)} : ();
    wantarray ? @a : shift @a;
}

sub send_event { shift->sendpdu(@_) }
sub send_query {
    my($self, @args) = @_;
    my $w = $self->_w;
    my $r = $self->_r;
    return unless defined $w;
    print $w sprintf "%s\n", $self->json->encode(\@args);
    my $a = <$r>;
    my @a = defined $a && $a ne '' ? @{from_json($a)} : ();
    wantarray ? @a : shift @a;
}


sub _w { $_[0]->{_w} }
sub _r { $_[0]->{_r} }

sub backend {
    my($pkg, $group, $cbk, $r, $w, @c) = @_;
    map { close($_) } @c;
    my $self = shift->SUPER::new(defined $group ? $group : 'shared');
    $self->{_json} = JSON->new->indent(0)->utf8(1);
    $self->{_shd} => {};

    select((select($r), $|=1)[0]);
    select((select($w), $|=1)[0]);    
    $SIG{PIPE} = sub {};
    
    my $cfd = $self->fd_get;
    
    while(!$self->finished) {
        my $rfd = fileno($r) if defined $r && $self->running;
        my $rout = my $wout = my $eout = my $rin = my $win = my $ein = '';
        my $timeout = 1;
        vec($rin,$rfd,1) = 1 if $rfd;
        vec($rin,$cfd,1) = 1;
	
        select($rout=$rin,$wout=$win,$eout=$ein,$timeout);
        $self->step if vec($rout,$cfd,1);
	if(vec($rout,$rfd,1)) {
	    my $a = [ ];
	    if(defined(my $q = <$r>)) {
		my(@q) = @{from_json($q)};
		my $x = $self->query(@q);
		$a = $x if defined $x;
	    } else {
		$self->finish;
		close($w);
	    }
	    printf $w "%s\n", $self->json->encode($a);
	}
    }
    return $self->error;
}

1;
