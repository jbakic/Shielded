
Shielded.ProxyGen generates transactional proxy subclasses. Your class should
have virtual properties with getters and setters. The proxy will override both.
Getter will not call base, but setter will, after it has changed the value in
the storage.

====

Shielded.ProxyGen is based on the work of Felice Pollano, titled "Automatic
Implementation of INotifyPropertyChanged on POCO Objects", available at:

    http://www.codeproject.com/Articles/141732/Automatic-Implementation-of-INotifyPropertyChanged

It is licenced under The Code Project Open License (CPOL), available at:

    http://www.codeproject.com/info/cpol10.aspx

(Links checked on date of writing: 2013-12-07)
