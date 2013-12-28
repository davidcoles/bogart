bogart - jointly share hashes between clients on a network

this is a simple collection of code for distributed, replicated data
stores. it consists of:

state.pl + fsm.pl => state.pm

  a finite state model and utility to generate a "use"able module that
  implements a shared state protocol on top of the corosync framework

sharer.pm

  a base class which abstracts away the intracies involved in dealing
  with the event based nature of using state.pm

bogart.pm + bogart.pl

  module and which demonstrate a local client/server shared hashing
  model and an extension to make the hashes distributed/replicated
  using the corosync based tools above

hv.pl

  a more dynamic demonstration of the shared state tools - see POD
  documentation at the bottom of the file.

you will need the corosync daemon installed and running. debian/ubuntu
works out of the box on a local machine once the /etc/default/corosync
file is updates to enable the daemon. you will also need the
development libraries and the corosync perl module (available from
https://github.com/cventers/perl-Corosync-CPG.git) installed.

any questions, just ask -- david.coles@potentialdeathtrap.net

