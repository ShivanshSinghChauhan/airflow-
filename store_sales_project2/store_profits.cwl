class: Workflow
cwlVersion: v1.0
id: store_profits
label: store_profits
$namespaces:
  sbg: 'https://www.sevenbridges.com/'
inputs:
  - id: check_file_exits
    'sbg:fileTypes': 'FASTQ, FQ, FASTQ.GZ, FQ.GZ, BAM, SAM'
    type: 'File[]'
    'sbg:x': -705.7030029296875
    'sbg:y': -202.24929809570312
outputs:
  - id: report_zip
    outputSource:
      - fastqc_0_11_4/report_zip
    'sbg:fileTypes': ZIP
    type: 'File[]?'
    'sbg:x': -181.6666717529297
    'sbg:y': -318.3333435058594
  - id: report_html
    outputSource:
      - fastqc_0_11_4/report_html
    'sbg:fileTypes': HTML
    type: 'File[]?'
    'sbg:x': -279.6666564941406
    'sbg:y': -11.333333015441895
steps:
  - id: fastqc_0_11_4
    in:
      - id: input_fastq
        source:
          - input_fastq
          - check_file_exits
    out:
      - id: report_zip
      - id: report_html
    run:
      cwlVersion: 'sbg:draft-2'
      class: CommandLineTool
      $namespaces:
        sbg: 'https://sevenbridges.com'
      id: admin/sbg-public-data/fastqc-0-11-4/9
      label: FastQC
      description: >-
        FastQC reads a set of sequence files and produces a quality control (QC)
        report from each one. These reports consist of a number of different
        modules, each of which will help identify a different type of potential
        problem in your data. 


        Since it's necessary to convert the tool report in order to show them on
        Seven Bridges platform, it's recommended to use [FastQC Analysis
        workflow
        instead](https://igor.sbgenomics.com/public/apps#admin/sbg-public-data/fastqc-analysis/). 


        FastQC is a tool which takes a FASTQ file and runs a series of tests on
        it to generate a comprehensive QC report.  This report will tell you if
        there is anything unusual about your sequence.  Each test is flagged as
        a pass, warning, or fail depending on how far it departs from what you
        would expect from a normal large dataset with no significant biases.  It
        is important to stress that warnings or even failures do not necessarily
        mean that there is a problem with your data, only that it is unusual. 
        It is possible that the biological nature of your sample means that you
        would expect this particular bias in your results.
      baseCommand:
        - fastqc
      inputs:
        - 'sbg:category': File inputs
          type:
            - type: array
              items: File
          inputBinding:
            position: 100
            separate: true
            'sbg:cmdInclude': true
          label: Input file
          description: Input file.
          'sbg:fileTypes': 'FASTQ, FQ, FASTQ.GZ, FQ.GZ, BAM, SAM'
          id: '#input_fastq'
        - 'sbg:altPrefix': '-f'
          'sbg:category': Options
          'sbg:toolDefaultValue': '7'
          type:
            - 'null'
            - int
          inputBinding:
            position: 0
            prefix: '--kmers'
            separate: true
            'sbg:cmdInclude': true
          label: Kmers
          description: >-
            Specifies the length of Kmer to look for in the Kmer content module.
            Specified Kmer length must be between 2 and 10. Default length is 7
            if not specified.
          id: '#kmers'
        - 'sbg:altPrefix': '-l'
          'sbg:category': File inputs
          type:
            - 'null'
            - File
          inputBinding:
            position: 0
            prefix: '--limits'
            separate: true
            'sbg:cmdInclude': true
            secondaryFiles: []
          label: Limits
          description: >-
            Specifies a non-default file which contains a set of criteria which
            will be used to determine the warn/error limits for the various
            modules.  This file can also be used to selectively remove some
            modules from the output all together.  The format needs to mirror
            the default limits.txt file found in the Configuration folder.
          'sbg:fileTypes': TXT
          id: '#limits_file'
        - 'sbg:altPrefix': '-a'
          'sbg:category': File inputs
          type:
            - 'null'
            - File
          inputBinding:
            position: 0
            prefix: '--adapters'
            separate: true
            'sbg:cmdInclude': true
            secondaryFiles: []
          label: Adapters
          description: >-
            Specifies a non-default file which contains the list of adapter
            sequences which will be explicity searched against the library. The
            file must contain sets of named adapters in the form
            name[tab]sequence.  Lines prefixed with a hash will be ignored.
          'sbg:fileTypes': TXT
          id: '#adapters_file'
        - 'sbg:altPrefix': '-c'
          'sbg:category': File inputs
          type:
            - 'null'
            - File
          inputBinding:
            position: 0
            prefix: '--contaminants'
            separate: true
            'sbg:cmdInclude': true
            secondaryFiles: []
          label: Contaminants
          description: >-
            Specifies a non-default file which contains the list of contaminants
            to screen overrepresented sequences against. The file must contain
            sets of named contaminants in the form name[tab]sequence.  Lines
            prefixed with a hash will be ignored.
          'sbg:fileTypes': TXT
          id: '#contaminants_file'
        - 'sbg:altPrefix': '-f'
          'sbg:category': Options
          'sbg:toolDefaultValue': FASTQ
          type:
            - 'null'
            - type: enum
              symbols:
                - bam
                - sam
                - bam_mapped
                - sam_mapped
                - fastq
              name: format
          inputBinding:
            position: 0
            prefix: '--format'
            separate: true
            'sbg:cmdInclude': true
          label: Format
          description: >-
            Bypasses the normal sequence file format detection and forces the
            program to use the specified format.  Valid formats are BAM, SAM,
            BAM_mapped, SAM_mapped and FASTQ.
          id: '#format'
        - 'sbg:category': Options
          type:
            - 'null'
            - boolean
          inputBinding:
            position: 0
            prefix: '--nogroup'
            separate: false
            'sbg:cmdInclude': true
          label: Nogroup
          description: >-
            Disable grouping of bases for reads >50bp. All reports will show
            data for every base in the read.  WARNING: Using this option will
            cause fastqc to crash and burn if you use it on really long reads,
            and your plots may end up a ridiculous size. You have been warned.
          id: '#nogroup'
        - 'sbg:category': Options
          type:
            - 'null'
            - boolean
          inputBinding:
            position: 0
            prefix: '--nano'
            separate: false
            'sbg:cmdInclude': true
          label: Nano
          description: >-
            Files come from naopore sequences and are in fast5 format. In this
            mode you can pass in directories to process and the program will
            take in all fast5 files within those directories and produce a
            single output file from the sequences found in all files.
          id: '#nano'
        - 'sbg:category': Options
          type:
            - 'null'
            - boolean
          inputBinding:
            position: 0
            prefix: '--casava'
            separate: false
            'sbg:cmdInclude': true
          label: Casava
          description: >-
            Files come from raw casava output. Files in the same sample group
            (differing only by the group number) will be analysed as a set
            rather than individually. Sequences with the filter flag set in the
            header will be excluded from the analysis. Files must have the same
            names given to them by casava (including being gzipped and ending
            with .gz) otherwise they won't be grouped together correctly.
          id: '#casava'
        - 'sbg:altPrefix': '-t'
          'sbg:category': Options
          'sbg:toolDefaultValue': '1'
          type:
            - 'null'
            - int
          inputBinding:
            position: 0
            prefix: '--threads'
            separate: true
            valueFrom:
              class: Expression
              engine: '#cwl-js-engine'
              script: |-
                {
                //if "threads" is not specified
                //number of threads is determined based on number of inputs
                  if (! $job.inputs.threads){
                    $job.inputs.threads = [].concat($job.inputs.input_fastq).length
                  }
                  return Math.min($job.inputs.threads,7)
                }
            'sbg:cmdInclude': true
          label: Threads
          description: >-
            Specifies the number of files which can be processed
            simultaneously.  Each thread will be allocated 250MB of memory so
            you shouldn't run more threads than your available memory will cope
            with, and not more than 6 threads on a 32 bit machine.
          id: '#threads'
        - 'sbg:altPrefix': '-q'
          'sbg:category': Options
          type:
            - 'null'
            - boolean
          inputBinding:
            position: 0
            prefix: '--quiet'
            separate: true
            'sbg:cmdInclude': true
          label: Quiet
          description: Supress all progress messages on stdout and only report errors.
          id: '#quiet'
        - 'sbg:category': Execution parameters
          'sbg:toolDefaultValue': Determined by the number of input files
          type:
            - 'null'
            - int
          label: Number of CPUs.
          description: Number of CPUs to be allocated per execution of FastQC.
          id: '#cpus_per_job'
        - 'sbg:category': Execution parameters
          'sbg:toolDefaultValue': Determined by the number of input files
          type:
            - 'null'
            - int
          label: Amount of memory allocated per job execution.
          description: Amount of memory allocated per execution of FastQC job.
          id: '#memory_per_job'
      outputs:
        - type:
            - 'null'
            - type: array
              items: File
          label: Report zip
          description: Zip archive of the report.
          'sbg:fileTypes': ZIP
          outputBinding:
            glob: '*_fastqc.zip'
            'sbg:metadata':
              __inherit__: input_fastq
            'sbg:inheritMetadataFrom': '#input_fastq'
          id: '#report_zip'
        - type:
            - 'null'
            - type: array
              items: File
          label: Report HTMLs
          description: FastQC reports in HTML format.
          'sbg:fileTypes': HTML
          outputBinding:
            glob: '*.html'
            'sbg:inheritMetadataFrom': '#input_fastq'
          id: '#report_html'
      requirements:
        - class: ExpressionEngineRequirement
          id: '#cwl-js-engine'
          requirements:
            - class: DockerRequirement
              dockerPull: rabix/js-engine
      hints:
        - class: 'sbg:CPURequirement'
          value:
            class: Expression
            engine: '#cwl-js-engine'
            script: |-
              {
                // if cpus_per_job is set, it takes precedence
                if ($job.inputs.cpus_per_job) {
                  return $job.inputs.cpus_per_job 
                }
                // if threads parameter is set, the number of CPUs is set based on that parametere
                else if ($job.inputs.threads) {
                  return $job.inputs.threads
                }
                // else the number of CPUs is determined by the number of input files, up to 7 -- default
                else return Math.min([].concat($job.inputs.input_fastq).length,7)
              }
        - class: 'sbg:MemRequirement'
          value:
            class: Expression
            engine: '#cwl-js-engine'
            script: |+
              {
                // if memory_per_job is set, it takes precedence
                if ($job.inputs.memory_per_job){
                  return $job.inputs.memory_per_job
                }
                // if threads parameter is set, memory req is set based on the number of threads
                else if ($job.inputs.threads){
                  return 1024 + 300*$job.inputs.threads
                }
                // else the memory req is determined by the number of input files, up to 7 -- default
                else return (1024 + 300*Math.min([].concat($job.inputs.input_fastq).length,7))
              }

        - class: DockerRequirement
          dockerImageId: 759c4c8fbafd
          dockerPull: 'images.sbgenomics.com/mladenlsbg/fastqc:0.11.4'
      arguments:
        - position: 0
          prefix: ''
          separate: true
          valueFrom: '--noextract'
        - position: 0
          prefix: '--outdir'
          separate: true
          valueFrom: .
      'sbg:appVersion':
        - 'sbg:draft-2'
      'sbg:categories':
        - FASTQ-Processing
        - Quality-Control
        - Quantification
      'sbg:cmdPreview': >-
        fastqc  --noextract --outdir .  /path/to/input_fastq-1.fastq 
        /path/to/input_fastq-2.fastq
      'sbg:content_hash': null
      'sbg:contributors':
        - mladenlSBG
        - admin
        - djordje_klisic
      'sbg:createdBy': mladenlSBG
      'sbg:createdOn': 1447773725
      'sbg:id': admin/sbg-public-data/fastqc-0-11-4/9
      'sbg:image_url': null
      'sbg:job':
        allocatedResources:
          cpu: 2
          mem: 1624
        inputs:
          cpus_per_job: null
          format: null
          input_fastq:
            - class: File
              path: /path/to/input_fastq-1.fastq
              secondaryFiles: []
              size: 0
            - class: File
              path: /path/to/input_fastq-2.fastq
              secondaryFiles: []
              size: 0
          memory_per_job: null
          quiet: true
          threads: null
      'sbg:latestRevision': 9
      'sbg:license': GNU General Public License v3.0 only
      'sbg:links':
        - id: 'http://www.bioinformatics.babraham.ac.uk/projects/fastqc/'
          label: Homepage
        - id: >-
            http://www.bioinformatics.babraham.ac.uk/projects/fastqc/fastqc_v0.11.4_source.zip
          label: Source Code
        - id: 'https://wiki.hpcc.msu.edu/display/Bioinfo/FastQC+Tutorial'
          label: Wiki
        - id: >-
            http://www.bioinformatics.babraham.ac.uk/projects/fastqc/fastqc_v0.11.4.zip
          label: Download
        - id: 'http://www.bioinformatics.babraham.ac.uk/projects/fastqc'
          label: Publication
      'sbg:modifiedBy': admin
      'sbg:modifiedOn': 1529402637
      'sbg:project': admin/sbg-public-data
      'sbg:projectName': SBG Public data
      'sbg:publisher': sbg
      'sbg:revision': 9
      'sbg:revisionNotes': HTML output added.
      'sbg:revisionsInfo':
        - 'sbg:modifiedBy': mladenlSBG
          'sbg:modifiedOn': 1447773725
          'sbg:revision': 0
          'sbg:revisionNotes': null
        - 'sbg:modifiedBy': djordje_klisic
          'sbg:modifiedOn': 1459957440
          'sbg:revision': 1
          'sbg:revisionNotes': null
        - 'sbg:modifiedBy': admin
          'sbg:modifiedOn': 1471861482
          'sbg:revision': 2
          'sbg:revisionNotes': null
        - 'sbg:modifiedBy': admin
          'sbg:modifiedOn': 1476440178
          'sbg:revision': 3
          'sbg:revisionNotes': Input categories added.
        - 'sbg:modifiedBy': admin
          'sbg:modifiedOn': 1476440178
          'sbg:revision': 4
          'sbg:revisionNotes': >-
            FASTQ input changed from single file to array. Added better thread
            handling. 


            IMPORTANT NOTICE: If updating this tool in existing workflow, it's
            necessary to REMOVE SCATTER (uncheck it) from input_fastq or it
            might break the pipeline.
        - 'sbg:modifiedBy': admin
          'sbg:modifiedOn': 1476440178
          'sbg:revision': 5
          'sbg:revisionNotes': >-
            FASTQ input changed from single file to array. Added better thread
            handling.


            IMPORTANT NOTICE: If updating this tool in existing workflow, it's
            necessary to REMOVE SCATTER (uncheck it) from input_fastq or it
            might break the pipeline.
        - 'sbg:modifiedBy': admin
          'sbg:modifiedOn': 1489067520
          'sbg:revision': 6
          'sbg:revisionNotes': >-
            IMPORTANT NOTICE: If updating this tool in existing workflow, it's
            necessary to REMOVE SCATTER (uncheck it) from input_fastq or it
            might break the pipeline."


            Added automatised handling of BAM and SAM files. Also, added
            security measures for better automated threading handling.
        - 'sbg:modifiedBy': admin
          'sbg:modifiedOn': 1489067520
          'sbg:revision': 7
          'sbg:revisionNotes': >-
            Changed the file types of limits, adapters and contaminants files to
            be TXT, they have to be in format name[tab]sequence. Format should
            be similar to the one in the Configuration folder provided with
            FastQC, txt files.


            "IMPORTANT NOTICE: If updating this tool in existing workflow, it's
            necessary to REMOVE SCATTER (uncheck it) from input_fastq or it
            might break the pipeline."
        - 'sbg:modifiedBy': admin
          'sbg:modifiedOn': 1493298709
          'sbg:revision': 8
          'sbg:revisionNotes': >-
            * Fixed the JS expression for the CPU and Memory allocation

            * Added cpus_per_job and memory_per_job parameters

            * Removed default version for format, so the tool can handle
            combinations of file formats
        - 'sbg:modifiedBy': admin
          'sbg:modifiedOn': 1529402637
          'sbg:revision': 9
          'sbg:revisionNotes': HTML output added.
      'sbg:sbgMaintained': false
      'sbg:toolAuthor': Babraham Institute
      'sbg:toolkit': FastQC
      'sbg:toolkitVersion': 0.11.4
      'sbg:validationErrors': []
    label: FastQC
    'sbg:x': -414
    'sbg:y': -199.3333282470703
  - id: sbg_html2b64
    in: []
    out:
      - id: b64html
    run:
      cwlVersion: 'sbg:draft-2'
      class: CommandLineTool
      $namespaces:
        sbg: 'https://sevenbridges.com'
      id: admin/sbg-public-data/sbg-html2b64/8
      label: SBG Html2b64
      description: >-
        Tool for converting HTML reports of FastQC, SnpEff, MultiQC (simple
        report only) and ChimeraScan to b64html so it can easily be displayed on
        SBG platform.
      baseCommand:
        - python
        - sbg_html_to_b64.py
      inputs:
        - 'sbg:category': File input.
          type:
            - 'null'
            - File
          inputBinding:
            position: 0
            prefix: '--input'
            separate: true
            'sbg:cmdInclude': true
            secondaryFiles: []
          label: Input file
          description: Compressed archive.
          'sbg:fileTypes': 'ZIP, HTML'
          id: '#input_file'
      outputs:
        - type:
            - 'null'
            - File
          label: B64html
          description: 'Output file, b64html.'
          'sbg:fileTypes': 'HTML, B64HTML'
          outputBinding:
            glob: '*b64html'
            'sbg:inheritMetadataFrom': '#input_file'
          id: '#b64html'
      requirements:
        - class: CreateFileRequirement
          fileDef:
            - filename: sbg_html_to_b64.py
              fileContent: |-
                """
                Usage:
                    sbg_html_to_b64.py --input FILE [--select FILE]

                Description:
                    This tool is used for conversion of html file to b64 html file so it can be easily displayed in browsers.

                Options:
                    -h, --help      Show this help message and exit. (For third class of tools it's required to put
                                    this option).

                    -v, --version   Show version and exit.

                    --input FILE    Input file is archive containing html and all other files included in the html file(images, etc).

                    --select FILE If we wish to select specific html file from folder that we wish to parse.

                Examples:
                    python sbg_html_to_b64.py --input sample_fastqc.zip
                """

                import os
                from docopt import docopt
                import os.path
                import base64
                import mimetypes
                from bs4 import BeautifulSoup
                from path import Path
                from subprocess import call, check_output
                import re


                def dataurl(data, img=False, mime=None):
                    isfile = os.path.isfile(data)
                    if not isfile and not mime:
                        raise Exception('Mimetype must be provided when encoding data is not a valid file path.')
                    if not mime:
                        mimetypes.init()
                        mime, enc = mimetypes.guess_type(os.path.join('file://', data))
                        if mime is None:
                            raise Exception('rfc2397: failed to determine file type')
                    if isfile:
                        with open(data, 'r') as fpp:
                            data = fpp.read()
                    if not img:
                        return 'data:%s;base64,%s' % (mime, base64.b64encode(data.encode(encoding="utf-8", errors="ignore")))
                    else:
                        return 'data:%s;base64,%s' % (mime, base64.b64encode(data))


                def compact_html(html_file):
                    with open(html_file) as f:
                        html = f.read()

                    if 'snpEff_summary' in html_file:
                        for l in html.split('\n'):
                            if str(l).startswith('<a name'):
                                html = html.replace(str(l), str(l) + '</a>')
                        html = html.replace('<p>', '<p></p>')
                        html = html[:-358]
                        soup = BeautifulSoup(html, "html5lib")

                        js = "javascript: void(0); document.getElementById('%s').scrollIntoView(true);"
                        for anchor in soup.findAll('a'):
                            if 'href' in str(anchor):
                                if anchor['href'].startswith('#'):
                                    anchor['href'] = js % anchor['href'][1:]
                                else:
                                    anchor.decompose()
                            else:
                                anchor['id'] = anchor['name']

                        return soup.prettify()

                    else:
                        html = html.replace('&middot;', '.')
                        html = html.replace('&raquo;', '>>')
                        html = html.replace('&ge;', '>=')
                        html = html.replace('&gt;', '>')
                        html = html.replace('&lt;', '<')
                        html = html.replace('\xab', '<<')
                        html = html.replace('\xbb', '>>')
                        html = html.replace('\xc2', '')
                        html = html.replace('&le;', '<=')
                        html = html.replace('&mdash;', '--')
                        #html = re.sub(re.compile("/\*.*?\*/", re.DOTALL), "", html)
                        base_dir = os.path.split(html_file)[0]
                        soup = BeautifulSoup(html, "html5lib")
                        for img in soup.findAll('img'):
                            if img['src'].find('data:') == 0:
                                durl_img = img['src']
                            else:
                                durl_img = dataurl(os.path.join(base_dir, img['src']), img=True)
                            img['src'] = durl_img
                        return soup.prettify()


                def html_to_dataurl(html_file):
                    return dataurl(compact_html(html_file), img=False, mime='text/html')

                if __name__ == "__main__":
                    args = docopt(__doc__, version='1.0')
                    filename = args.get('--input')

                    # unzipping the archive
                    if Path(filename).ext == '.zip':
                        cmd = ["unzip", filename, "-d", "./unzip"]
                        call(cmd)

                        if args.get('--select'):
                            selected_file = args.get('--select')
                            filepath = "./unzip/" + args.get('--select')
                            if os.path.isfile(filepath):
                                html_file = filepath
                                b64_html = selected_file + '.b64html'
                            else:
                                raise Exception("File not present!")
                        else:
                            html_file = check_output(["find", "./unzip", "-iname", "*.html"]).split('\n')[:-1]
                            if len(html_file) == 1:
                                b64_html = Path(filename).namebase + '.b64html'
                                html_file = html_file[0]                                # conversion from list to string
                            else:
                                b64_html = [x.split('/')[-1] + '.b64html' for x in html_file]
                    else:
                        html_file = filename
                        b64_html = Path(filename).namebase + '.b64html'

                    # check if we need to process single or list of html files. if it is a single file then html_file is type string
                    if type(html_file) is str:
                        with open(b64_html, 'w') as fp:
                            fp.write(html_to_dataurl(html_file))
                    elif type(b64_html) is list:
                        for i, elem in enumerate(b64_html):
                            with open(elem, 'w') as fp:
                                print(html_file[i])
                                fp.write(html_to_dataurl(html_file[i]))
                    else:
                        raise Exception('This is not good.')
      hints:
        - class: 'sbg:CPURequirement'
          value: 1
        - class: 'sbg:MemRequirement'
          value: 1000
        - class: DockerRequirement
          dockerImageId: 8c35d2a2d8d1
          dockerPull: 'images.sbgenomics.com/medjo/sbg-html:1.0'
      'sbg:appVersion':
        - 'sbg:draft-2'
      'sbg:categories':
        - Converters
        - Plotting-and-Rendering
      'sbg:cmdPreview': python sbg_html_to_b64.py
      'sbg:content_hash': null
      'sbg:contributors':
        - mladenlSBG
        - admin
        - djordje_klisic
      'sbg:createdBy': mladenlSBG
      'sbg:createdOn': 1447773689
      'sbg:id': admin/sbg-public-data/sbg-html2b64/8
      'sbg:image_url': null
      'sbg:job':
        allocatedResources:
          cpu: 1
          mem: 1000
        inputs:
          input_file:
            class: File
            path: input_file.ext
            secondaryFiles: []
            size: 0
      'sbg:latestRevision': 8
      'sbg:license': Apache License 2.0
      'sbg:modifiedBy': admin
      'sbg:modifiedOn': 1503658884
      'sbg:project': admin/sbg-public-data
      'sbg:projectName': SBG Public data
      'sbg:publisher': sbg
      'sbg:revision': 8
      'sbg:revisionNotes': Difference between img and html in dataurl.
      'sbg:revisionsInfo':
        - 'sbg:modifiedBy': mladenlSBG
          'sbg:modifiedOn': 1447773689
          'sbg:revision': 0
          'sbg:revisionNotes': null
        - 'sbg:modifiedBy': djordje_klisic
          'sbg:modifiedOn': 1459977556
          'sbg:revision': 1
          'sbg:revisionNotes': null
        - 'sbg:modifiedBy': admin
          'sbg:modifiedOn': 1496321089
          'sbg:revision': 2
          'sbg:revisionNotes': 'MulitQC, ChimeraScan added'
        - 'sbg:modifiedBy': admin
          'sbg:modifiedOn': 1496321089
          'sbg:revision': 3
          'sbg:revisionNotes': null
        - 'sbg:modifiedBy': admin
          'sbg:modifiedOn': 1496670366
          'sbg:revision': 4
          'sbg:revisionNotes': null
        - 'sbg:modifiedBy': admin
          'sbg:modifiedOn': 1496674570
          'sbg:revision': 5
          'sbg:revisionNotes': bug fixed
        - 'sbg:modifiedBy': admin
          'sbg:modifiedOn': 1496674570
          'sbg:revision': 6
          'sbg:revisionNotes': null
        - 'sbg:modifiedBy': admin
          'sbg:modifiedOn': 1503052999
          'sbg:revision': 7
          'sbg:revisionNotes': data errors=ignore added
        - 'sbg:modifiedBy': admin
          'sbg:modifiedOn': 1503658884
          'sbg:revision': 8
          'sbg:revisionNotes': Difference between img and html in dataurl.
      'sbg:sbgMaintained': false
      'sbg:toolAuthor': Seven Bridges
      'sbg:toolkit': SBGTools
      'sbg:toolkitVersion': '1.0'
      'sbg:validationErrors': []
    label: SBG Html2b64
    'sbg:x': 1.3333333730697632
    'sbg:y': -177.3333282470703
requirements:
  - class: MultipleInputFeatureRequirement
