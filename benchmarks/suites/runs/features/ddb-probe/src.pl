use IO::Socket::INET; 
my ($per, @hosts) = @ARGV; my $t0=time(); my $buf = "x" x (1<<20); my @k;
for my $h (@hosts) { for (1..3) { my $p = fork; if (!$p) { my $s = IO::Socket::INET->new(PeerAddr=>$h, PeerPort=>17777) or die "$h: $!"; my $sent=0; while ($sent < $per) { syswrite($s,$buf); $sent += length $buf } close $s; exit 0 } push @k,$p } }
waitpid($_,0) for @k; printf "sent %d conns x %.2f GB in %.2fs\n", scalar(@k), $per/1e9, time()-$t0;
