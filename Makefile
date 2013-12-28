
all: state.pm

state.pm: state.pl; ./fsm.pl <state.pl >state.pm 

test: state.pm
	./cpg.pl -i </etc/group
	./cpg.pl -o
