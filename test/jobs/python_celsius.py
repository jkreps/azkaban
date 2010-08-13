#!/usr/bin/python

import re, string, sys

# if no arguments were given, print a helpful message
l=len(sys.argv)
if l < 1:
	print 'Usage: celsium --t temp'
	sys.exit(1)


# Loop over the arguments
i=1
while i < l-1 :
	name = sys.argv[i]
	value = sys.argv[i+1]

	if name == "--t":
		try:
			fahrenheit = float(string.atoi(value))
		except string.atoi_error:
			print repr(value), " not a numeric value"
		else:
			celsius=(fahrenheit-32)*5.0/9.0
			print '%i\260F = %i\260C' % (int(fahrenheit), int(celsius+.5))

		sys.exit(0)

	i=i+2



