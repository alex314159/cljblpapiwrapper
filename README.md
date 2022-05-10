# cljblpapiwrapper

Simple Clojure wrapper around the Bloomberg Java API

## Usage

This will only work if you're connected to Bloomberg, typically on a machine where the Bloomberg terminal application is running. The request/response paradigm and the subscription paradigm are currently implemented. Please check the examples.

Breaking change from 0.3.3 to 0.3.5: latest SDK returns dates with timezone so "yyyy-mm-dd+hh:mm" instead of "yyyy-mm-dd" previously.

## License

Copyright © 2019-2022 Alexandre Almosni

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
