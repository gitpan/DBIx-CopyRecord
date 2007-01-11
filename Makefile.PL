

use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME         => 'CopyRecord',
    VERSION_FROM => 'lib/CopyRecord.pm', # finds \$VERSION
    AUTHOR       => 'Jack Bilemjian (jck000@gmail.com)',
    ABSTRACT     => 'Copy Records within a database',
    PREREQ_PM    => {
                     'Test::Simple' => 0.44,
                    },
);
