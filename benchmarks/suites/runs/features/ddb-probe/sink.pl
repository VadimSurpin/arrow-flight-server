use IO::Socket::INET; $SIG{CHLD}='IGNORE';
my $l = IO::Socket::INET->new(Listen=>64, LocalPort=>17777, ReuseAddr=>1) or die;
alarm 120;
while (my $c = $l->accept) { next if fork; my $n=0; my $b; while (my $r = sysread($c,$b,1<<20)) { $n+=$r } exit 0 }
