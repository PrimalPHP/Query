#Primal.Query

Created and Copyright 2012-2013 by Jarvis Badgley, chiper at chipersoft dot com.

Primal.Query is a chain-able asynchronous query builder class which allows for easy construction and execution of complex queries with data escapement.

[Primal PHP](http://www.primalphp.com) is a collection of independent PHP micro-libraries.

##Requirements

Primal.Query requires PHP 5.3 (tested under 5.3.10).  Use of the built in execution methods (select(), insert(), delete(), etc) requires the PHP Data Objects (PDO) extension be installed and configured.


##Usage

A Query object can be initialized directly via the `new` operator, but the intended implementation is for initial properties to be chained off of a static initialization method.  Example:

```php
$q=Primal\Query\MySQL::Make($pdo) //$pdo contains your PDO link object
   ->from('users','u')
   ->leftJoin("user_billing b USING (user_id)")
   ->orderBy('u.name')
   ->returns('u.id', 'u.name', 'b.start_date')
   ->whereTrue('b.active')
   ->whereDateInRange('b.start_date', new DateTime('yesterday'));
$results = $q->select();
```

If you wish to run the query yourself instead of using the built in execution functions, the `build*` functions will return a tuple array containing the query text and an array of all named parameters.

##Documentation

Method documentation is still forthcoming. In the meantime please use the doc comments in the class itself.