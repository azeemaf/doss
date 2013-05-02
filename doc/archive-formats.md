Comparison of Archive Formats
=============================

|Detail                             |[POSIX tar]|[Zip64]   |[WARC]                      |
|-----------------------------------|-----------|----------|----------------------------|
|Tool availability                  |Good       |Moderate  |Poor                        |
|Max record size                    |Unlimited  |16 EB     |Unlimited                   |
|Max archive size                   |Unlimited  |16 EB     |Unlimited                   |
|Size with 10,000 empty files       |14 MB [1]  |14 MB     |1 MB [2]                    |
|Size with 1,000 32KB files         |33 MB      |32 MB     |32 MB                       |
|Table of contents                  |No         |Yes       |No                          |
|Digest / checksum field            |No         |CRC32     |any; SHA1 is most common    |
|Record segmentation                |No         |Yes       |Yes; poor tool support      |
|Metadata support                   |Moderate   |Poor      |Good                        |
|Complexity                         |Moderate   |Moderate  |Simple core [3] with many optional features |
|Time resolution                    |Arbitrary  |2 seconds | 1 second                   |

[1] Depends on blocking factor. 14 MB with GNU r --posix -b 1
[2] Depends on amount of metadata included. 1.4 MB (140 bytes per record) when only including mandatory fields.

[3] By design you can read a WARC in a text editor and understand it without having read the spec.  It's very similar to HTTP.

Thoughts
--------
For delivery it doesn't really matter -- anything we can construct an external offset index for is fine. If we were storing extremely small records (tens of bytes) something custom like [Haystack] might be better but the overheads for all three standard formats are reasonable for our purposes.

We're already using all three formats in different parts of the digital library. With each of them if we need to extend them we can insert our own metadata records as separate files.


[WARC]: http://bibnum.bnf.fr/WARC/warc_ISO_DIS_28500.pdf
[POSIX tar]: https://en.wikipedia.org/wiki/Tar_%28file_format%29
[Zip64]: https://en.wikipedia.org/wiki/Zip_(file_format)#ZIP64
[Haystack]: https://www.facebook.com/note.php?note_id=76191543919

