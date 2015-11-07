Shielded.ProxyGen generates transactional proxy subclasses. Your class should
have virtual properties with getters and setters. The proxy will override both.
Getter will not call base, but setter will, before it has changed the value in
the storage, allowing you to access the old value using the getter.

WARNING: Due to limitations of CodeDom, the getter and setter must be either
both public, or both protected. A mix will cause the Factory to throw an
exception.

If your class defines a method "public void Commute(Action)", this will get
overriden. The override will execute the received action as a commute. The action
may not access (not even indirectly) any other shielded field.

If the class has a protected method OnChanged(string), that will get called
by the proxy after every property change, with the property name as argument.

====

Shielded.ProxyGen is based on the work of Felice Pollano, titled "Automatic
Implementation of INotifyPropertyChanged on POCO Objects", available at:

    http://www.codeproject.com/Articles/141732/Automatic-Implementation-of-INotifyPropertyChanged

The Pollano work is licenced under The Code Project Open License (CPOL), available at:

    http://www.codeproject.com/info/cpol10.aspx

(Links checked on date of writing: 2013-12-07)
