# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [0.3.3]
### Changed
- Breaking change in bdp: overrides are now supplied as a map to allow for >1 override

## [0.3.4]
### Changed
- Update bbg SDK to latest version + update Clojure version
- Note that latest SDK returns dates with timezone "yyyy-mm-dd+hh:mm"
  
## [0.3.5]
### Changed
- Fixed bug in sapi session authorisation

## [0.3.6]
### Changed
- update to blpapi 3.25.2.1. Many strings replaced by Bloomberg Name classes as former methods deprecated.
- Bloomberg wants to sunset 'Server mode', session authentication changed slightly as a result. Identity is now necessary.