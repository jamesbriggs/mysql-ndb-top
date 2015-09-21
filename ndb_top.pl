#!/usr/bin/perl

# Program: ndb_top.pl
# Author: James Briggs, California, USA
# Date: 2015 09 20
# Purpose: basic real-time monitoring of multiple ndb clusters on one screen (tested with 20 clusters)
# Notes:
# - only one instance of this program should be run if you enable table counts for non-MyISAM storage engines
# - also need to connect to datanodes and get disk free space

use strict;
use diagnostics;

   my $title    = 'MySQL NDB Clusters';
   my $username = 'admin';
   my $password = 'admin';
   my $db_name  = 'db';

   my $cmd_mysql="/usr/local/mysql/bin/mysql --compress -A --connect-timeout=2";
   my $cmd_mysql_ndb_mgm="/usr/local/mysql/bin/ndb_mgm";
   my $cmd_ssh="ssh -o 'StrictHostKeyChecking=no'";

   use constant TM_DELAY   => 10; # pause in seconds
   use constant ADD_GRANTS => 1;  # optionally used to add grants as needed
   use constant MAX_DISK   => '80%';

   my %hosts = (
      node1 => '10.0.0.2',
      node2 => '10.0.0.12',
      node3 => '10.0.0.22',
      node4 => '10.0.0.32',
   );

   my %notes = (
      node3 =>  'under repair',
   );
   
   my %blued_out = (
      node3 =>  1,
   );
   
   my $password_root = 'mysqlrootpass'; # this is optionally used to add grants as needed

   my %tables = (
      # db.table => expected row count,
      'db.table1' => 100,
  );

  my @tables = sort keys %tables;

while (1) {
   `reset`;
   if ($title ne '') {
      print "$title @{[ scalar localtime ]}\n\n";
   }

# order the hosts however you like here ...
   my @hosts = sort keys %hosts;

   for my $cluster (@hosts) {
      my ($datanode1, $datanode2, $host) = ndb_mgm_parse($hosts{$cluster});
      next if $host eq '';

      print "$cluster $host";
      print " ($notes{$cluster})" if exists $notes{$cluster};

      if (exists $blued_out{$cluster}) {
         print_blue("\tnot monitored\n\n");
         next;
      }

      my $rtables  = \@tables;
      my $rhtables = \%tables;

      my $out=`$cmd_mysql -u $username -p$password -h $host -e "SHOW GLOBAL STATUS LIKE 'Uptime'; SHOW GLOBAL STATUS LIKE 'Max_used_connections'" 2>&1`; # do a simple query to see if node up
      if ($out =~ /ERROR 1296/s) {
         print_red("\tCluster down\n\n");
      }
      elsif ($out =~ /ERROR 2003/s) {
         print_red("\tSQL node down\n\n");
      }
      elsif ($out =~ /ERROR 1045/s) {
         print_red("\tInvalid login for $username?\n\n");
         if (ADD_GRANTS) {
            $out = `$cmd_ssh $host "mysql -u root -p$password_root -e 'grant all on ${db_name}.* to \'$username\'@\'%\' identified by \'$password\''"`;
         }
      }
      else {
         if ($out =~ /(\d+).*?(\d+)/s) {
            printf(" Uptime %.1f days, Max connections %d, Disk used ", $1/86400, $2);
            $out = get_disk_usage($datanode1, '/');
            fmt_disk_usage($out); print "/";

            $out = get_disk_usage($datanode2, '/');
            fmt_disk_usage($out);
         }
         print "\n";

         for my $table (@$rtables) {
            my $cmd = qq[$cmd_mysql -u $username -p$password -h $host -e "select count(*) as '$table' from $table" 2>&1];
            $out=`$cmd`;
            $out = mysql_clean_output($out);
            $out =~ s/ERROR 1146 .*?\.(\w+)' doesn't exist/$1\nNA/g;
            $out =~ s/\n/\t/g;

            if ($out =~ /\b(\d+|NA)\b/) {
               no warnings;
               if ($1 == 0 || $1 eq 'NA' || $rhtables->{$table} != $1) {
                  print_red($out);
               }
               else {
                  print $out;
               }
            }

         }
         print "\n";
         $out=`$cmd_mysql -u $username -p$password -h $host -e "show global status like 'ndb_number_of%'" 2>&1`;
         $out = mysql_clean_output($out);
         $out =~ s/Variable_name\tValue//i;
         $out =~ s/\n//g;
         $out =~ s/(Ndb_number_of_data_nodes)\s+(\d+)\s*(Ndb_number_of_ready_data_nodes)\s+(\d+)/$1\t$2\t$3\t$4/;
         if ($2 == 0 or $4 == 0) {
            print_red("\t$out\n");
         }
         else {
            print "\t$out\n";
         }
         $out = `$cmd_ssh $host "$cmd_mysql_ndb_mgm -e 'all report MemoryUsage'"`;
         $out =~ s/Connected to Management Server at: (.*?):1186\n//s;
         $out =~ s/\n/\n\t/g;
         chop $out;
         if ($out =~ /\b\([5-9]{2}%\)/) { # only print DataMemory and IndexMemory if > 50% used
            print "\t$out";
         }
         else {
            print "\n" unless $hosts[-1] eq $cluster;
         }
      }
   }
   sleep TM_DELAY;
}

sub mysql_clean_output {
   my ($in) = @_;

   $in =~ s/mysql: \[Warning\] Using a password on the command line interface can be insecure\.//i;

   return $in;
}

exit;

sub print_red {
   my ($in) = @_;

   printf("%c[1;31m%s%c[0m", 27, $in, 27); # red
}

sub print_blue {
   my ($in) = @_;

   printf("%c[1;34m%s%c[0m", 27, $in, 27); # blue
}

sub print_green {
   my ($in) = @_;

   printf("%c[1;32m%s%c[0m", 27, $in, 27); # green
}

sub ndb_mgm_parse {
   my ($host_ndb_mgm) = @_;

   my $datanode1 = '';
   my $datanode2 = '';
   my $sql1 = '';

   my $out = `$cmd_ssh $host_ndb_mgm "$cmd_mysql_ndb_mgm -e show"`;

no warnings;

   open(IN, '<', \$out);
      while (<IN>) {
         last if /ndbd\(NDB\)/;
      }
      my $in = <IN>;
      ($datanode1) = $in =~ /@([^ ]+) /;
      $in = <IN>;
      ($datanode2) = $in =~ /@([^ ]+) /;

      while (<IN>) {
         last if /mysqld\(API\)/;
      }
      while (<IN>) {
        if (/@([^ ]+) /) {
           $sql1 = $1;
           last;
        }
      }
      close IN;

   return ($datanode1, $datanode2, $sql1);
}

sub get_disk_usage {
   my ($host, $fs) = @_;

   my $used = 0;
   my $out = `$cmd_ssh $host "df -h $fs 2>&1"`;

   if ($out =~ /(\d+%)/gs) {
      $out = sprintf("%s", $1);
      return $1;
   }

   return "NA";
}

sub fmt_disk_usage {
   my ($in) = @_;

   if ($in =~ /(\d+%)/gs) {
      if ($1 lt MAX_DISK) {
         print $in;
      }
      else {
         print_red($in);
      }
   }
   else {
      print_red($in);
   }
}
