#!/usr/bin/perl
use strict;

my %state;
my %event;

while(<>) { last if /^__END__/; print }
while(<>) {
    next if /^\043/;
    if(/\s*(\S+)\s+(\S+)\s+(\S+)\s*(\S+|)/) {
	my($c, $e, $n, $a) = ($1, $2, $3, $4);
	$e = undef if $e eq '*';
	$n = undef if $n eq '*';
	$a = undef if $a eq '';
	
	$e = 'ANY' unless defined $e;
	$a = 'NUL' unless defined $a;
	$n = $c    unless defined $n;

	$state{$c}{$e}{_n} = $n;
	$state{$c}{$e}{_a} = $a;
	$event{$e} = 1;
    } else {
    }
}

#foreach(sort keys(%state)) { print "$_\n" }
#foreach(sort keys(%event)) { print "$_\n" }
#exit;

print "sub _fsm {\n";
print '  my($self, $s, $e, @a) = @_;'."\n";
printf '  switch($s) {'."\n";
foreach my $s (sort keys(%state)) {
    printf '    case "%s" {'."\n", $s;
    printf '      switch($e) {'."\n";
    foreach my $e (sort keys(%{$state{$s}})) {
	next if $e eq 'ANY';
	printf '        case "%s" {', $e;
	my $n = $state{$s}{$e}{_n};
	my $a = $state{$s}{$e}{_a};
	printf ' return ("%s"', $n;
	if($a ne 'NUL') {
	    printf ', $self->%s(@a))', $a;
	} else {
	    print  ")";
	}
	printf " }\n";
    }
    printf "      }\n";
    if(defined $state{$s}{'ANY'}) {
	printf "      return ('%s');\n", $state{$s}{'ANY'}{_n};
    } else {
	printf "      return ('%s');", $s;
    }
    printf "    }\n";
}
printf "  }\n}\n";

print <DATA>;


__END__;
sub fsm {
    my($self, $e, @a) = @_;
    $self->{_STATE} = 'START' unless defined $self->{_STATE};
    my $s = $self->{_STATE};
    warn "<$s, $e [$a[0]]\n" if $ENV{DEBUG}>2;
    my($n, @e) = $self->_fsm($s, $e, @a);
    warn "=$n\n" if $ENV{DEBUG}>2;
    $self->{_STATE} = $n;
    foreach(@e) { $self->fsm(@$_) }
}
1;


