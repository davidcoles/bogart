#!/usr/bin/perl
package state;
use strict;
use Switch;
use JSON;
use Corosync::CPG qw/:constants/;
use base qw(Corosync::CPG);
use Digest::MD5 qw(md5_hex);
our $SELF;

sub confchg       {} # to be notified about leave/join events
sub process_event {} # to process events generated by PDUs
sub initialise    {} # called when application joins the group
sub serialise     {} # serialse the application state to be copied to a replica
sub deserialise   {} # deserialise the application state from a replica
sub dump          { {} }

sub reset     { $_[0]->{_STATE} = 'START'; $_[0]->initialise; warn "RESET\n" }
sub nodeid    { $_[0]->local_get .":$$" }
sub md5       { $_[0]->{_md5} }
sub running   { $_[0]->{_STATE} eq 'RUNNING' ? 1 : 0 }
sub error     { $_[0]->{_STATE} eq 'ERROR)'  ? 1 : 0 }
sub finished  { $_[0]->{_STATE} =~ /^(FINISHED|ERROR)$/ ? 1 : 0 }
sub finish    { $_[0]->fsm('FIN_RECV') }
sub step      { $_[0]->SUPER::dispatch(CS_DISPATCH_ALL) }
sub sendpdu   { $_[0]->sendmsg('PDU', undef, $_[1]) }
sub sendmsg   {
    my($self, $type, $unto, $body) = @_;
    my $msg = to_json({ type => $type, unto => $unto, body => $body });
    warn sprintf ">%s %s\n", $type, substr($msg, 0, 75) if $ENV{DEBUG} > 1;
    $self->mcast_joined(CPG_TYPE_AGREED, $msg);
}

sub members {
    my($self, @del) = @_;
    foreach(@del) { delete $self->{_wai}->{$_} };
    scalar keys %{$self->{_wai}};
}

sub new {
    my($pkg, $group) = @_;
    $group = 'default' unless defined $group;
    $SELF = my $self = $pkg->SUPER::new(callbacks => {
	deliver => \&deliver_callback, confchg => \&confchg_callback });
    $self->join($group);

    # seems it's possible to bind to 127.0.0.1
    if($self->nodeid =~ /^16777343:/ && ! -t \*STDIN && !defined $ENV{DEBUG}) {
	warn "16777343 - bound to localhost - set DEBUG env var to ignore\n";
	sleep 5;
	exit;
    }

    $self->{_que} = [];
    $self->{_wai} = {};
    $self;
}

sub deliver_callback {
    my($g, $n, $p, $m, $self) = @_; $self = $SELF unless defined $self;
    my($type,$unto,$body) = @{from_json($m)}{qw(type unto body)};
    
    return if defined $unto and $unto ne $self->nodeid; # not to me ..,
    
    my $req_event = "$n:$p" eq $self->nodeid ? 'REQ_SELF' : 'REQ_OTHR';	
    $req_event = 'REQ_WAIT' if exists $self->{_wai}{"$n:$p"};

    switch($type) {
	case 'REQ' { $self->fsm($req_event, "$n:$p") }
	case 'NAK' { $self->fsm('NAK_RECV', "$n:$p") }
	case 'INI' { $self->fsm('INI_RECV', "$n:$p", @$body) }
	case 'ACK' { $self->fsm('ACK_RECV', "$n:$p", @$body) }
	case 'PDU' { $self->fsm('PDU_RECV', "$n:$p", $body, $m) }
	case 'ENQ' { warn sprintf "ENQ %s\n", $self->md5 }
	case 'DMP' { warn sprintf "DMP %s\n", $self->dump }
	else { die sprintf ">> %s %s\n", $type, $self->serialise }
    }
}

sub confchg_callback { # called when a node joins or leaves the group
    my($group, $members, $leaving, $joining, $self) = @_;
    $self = $SELF unless defined $self;
    my @members = sort map { join(':', $_->{nodeid}, $_->{pid}) } @$members;
    my @leaving = sort map { join(':', $_->{nodeid}, $_->{pid}) } @$leaving;
    my @joining = sort map { join(':', $_->{nodeid}, $_->{pid}) } @$joining; 
    my $nodeid = $self->nodeid;

    %{$self->{_mem}} = map { $_ => 1 } grep !/^$nodeid$/, @members;
    
    foreach(@leaving) {$self->fsm($_ eq $nodeid ? 'SLF_LEAV' : 'OTH_LEAV', $_)}
    foreach(@joining) {$self->fsm($_ eq $nodeid ? 'SLF_JOIN' : 'OTH_JOIN', $_)}
    $self->confchg($group, [@members], [@leaving], [@joining]);
}


#############################################################################
sub SEND_ACK { $_[0]->sendmsg('ACK',$_[1],[$_[0]->md5,$_[0]->serialise]); () }
sub SEND_NAK { $_[0]->sendmsg('NAK',$_[1]); () }
sub SEND_REQ { $_[0]->sendmsg('REQ'); () }
sub RECV_NAK { $_[0]->members($_[1]) ? () : (['NUL_LIST']) }
sub PUSH_PDU { push(@{shift->{_que}}, [@_]); () }

sub SEND_INI {
    my($self) = @_;
    $self->initialise;
    $self->{_que} = [];
    $self->sendmsg('INI', undef, [ md5_hex($self->nodeid),$self->serialise ]);
    return ();
}

sub RECV_ACK {
    my($self, $from, $md5, $shd) = @_;  warn "ACK<$md5 $from\n" if $ENV{DEBUG};
    $self->{_wai} = {};
    $self->{_md5} = $md5;
    $self->deserialise($shd);
    map { $self->PROC_PDU(@$_) } @{$self->{_que}};
    $self->{_que} = [];
    return ();
}

sub PROC_PDU {
    my($self, $from, $body, $m) = @_;
    $self->{_md5} = md5_hex($self->md5.$m);
    $self->process_event($body, $from);
    return ();
}

sub STRT_QUE {
    my($self) = @_;
    $self->{_que} = [];
    $self->{_wai} = $self->{_mem};
    return $self->members ? () : (['NUL_LIST']);
}

__END__;
#STATE		EVENT           NEW STATE       ACTION
START		SLF_JOIN	REQ_SENT	SEND_REQ
START		*		START		

REQ_SENT	REQ_SELF	REQ_RCVD	STRT_QUE
REQ_SENT	REQ_OTHR	REQ_SENT	SEND_NAK
REQ_SENT	SLF_LEAV	FINISHED		
REQ_SENT	*		REQ_SENT

REQ_RCVD	NAK_RECV	REQ_RCVD	RECV_NAK
REQ_RCVD	OTH_LEAV	REQ_RCVD	RECV_NAK
REQ_RCVD	REQ_WAIT	REQ_RCVD	SEND_NAK
REQ_RCVD	PDU_RECV	REQ_RCVD	PUSH_PDU
REQ_RCVD	INI_RECV	RUNNING		RECV_ACK
REQ_RCVD	ACK_RECV	RUNNING		RECV_ACK
REQ_RCVD	NUL_LIST	INI_SENT	SEND_INI
REQ_RCVD	REQ_OTHR	REQ_RCVD
REQ_RCVD	OTH_JOIN	REQ_RCVD
REQ_RCVD	SLF_LEAV	FINISHED		
REQ_RCVD	*		ERROR

INI_SENT	INI_RECV	RUNNING		RECV_ACK
INI_SENT	ACK_RECV	ERROR
INI_SENT	SLF_LEAV	FINISHED
INI_SENT        *               INI_SENT

RUNNING		REQ_OTHR	RUNNING		SEND_ACK
RUNNING		PDU_RECV	RUNNING		PROC_PDU
RUNNING         SLF_LEAV        FINISHED
RUNNING         FIN_RECV        FINISHED
RUNNING		*		RUNNING

FINISHED	*		FINISHED
ERROR           *               ERROR
