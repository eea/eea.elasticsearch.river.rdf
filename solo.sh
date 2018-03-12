#!/usr/bin/perl -s
#
# solo v1.7
# Prevents multiple cron instances from running simultaneously.
#
# Copyright 2007-2016 Timothy Kay
# http://timkay.com/solo/
#
# It is free software; you can redistribute it and/or modify it under the terms of either:
#
# a) the GNU General Public License as published by the Free Software Foundation;
#    either version 1 (http://dev.perl.org/licenses/gpl1.html), or (at your option)
#    any later version (http://www.fsf.org/licenses/licenses.html#GNUGPL), or
#
# b) the "Artistic License" (http://dev.perl.org/licenses/artistic.html), or
#
# c) the MIT License (http://opensource.org/licenses/MIT)
#

use Socket;

alarm $timeout								if $timeout;

$port =~ /^\d+$/ or $noport						or die "Usage: $0 -port=PORT COMMAND\n";

if ($port)
{
    # To work with OpenBSD: change to
    # $addr = pack(CnC, 127, 0, 1);
    # but make sure to use different ports across different users.
    # (Thanks to  www.gotati.com .)
    $addr = pack(CnC, 127, $<, 1);
    print "solo: bind ", join(".", unpack(C4, $addr)), ":$port\n"	if $verbose;

    $^F = 10;			# unset close-on-exec

    socket(SOLO, PF_INET, SOCK_STREAM, getprotobyname('tcp'))		or die "socket: $!";
    bind(SOLO, sockaddr_in($port, $addr))				or $silent? exit: die "solo($port): $!\n";
}

sleep $sleep if $sleep;

exec @ARGV;