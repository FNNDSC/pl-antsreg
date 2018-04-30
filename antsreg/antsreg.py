#                                                            _
# antsreg ds app
#
# (c) 2016 Fetal-Neonatal Neuroimaging & Developmental Science Center
#                   Boston Children's Hospital
#
#              http://childrenshospital.org/FNNDSC/
#                        dev@babyMRI.org
#

import os
import sys
import subprocess
import time
from filelock import FileLock

# Import the Chris app superclass
from chrisapp.base import ChrisApp

class AntsReg(ChrisApp):
    """
    A plugin app for registration using ants.
    """
    AUTHORS         = 'FNNDSC (dev@babyMRI.org)'
    SELFPATH        = os.path.dirname(os.path.abspath(__file__))
    SELFEXEC        = os.path.basename(__file__)
    EXECSHELL       = 'python3'
    TITLE           = 'Ants registration'
    CATEGORY        = ''
    TYPE            = 'ds'
    DESCRIPTION     = 'A plugin app for registration using ants'
    DOCUMENTATION   = 'http://wiki'
    VERSION         = '0.1'
    LICENSE         = 'Opensource (MIT)'
    MAX_NUMBER_OF_WORKERS = 1024
    MIN_NUMBER_OF_WORKERS = 1
    MAX_CPU_LIMIT         = '1000m'
    MIN_CPU_LIMIT         = '1000m'
    MAX_MEMORY_LIMIT      = '10Gi'
    MIN_MEMORY_LIMIT      = '1Gi'
    MIN_GPU_LIMIT = 0  # Override with the minimum number of GPUs, as an integer, for your plugin
    MAX_GPU_LIMIT = 0  # Override with the maximum number of GPUs, as an integer, for your plugin

    # Fill out this with key-value output descriptive info (such as an output file path
    # relative to the output dir) that you want to save to the output meta file when
    # called with the --saveoutputmeta flag
    OUTPUT_META_DICT = {}
 
    # Signals for interworker communication
    IDLE  = 0
    START = 1
    EXIT  = 2

    # Files for interworker communication 
    tmp_path                  = ''      # temporary folder to be removed at end of execution
    worker_num_file_path      = ''      # file used to distribute worker numbers
    worker_num_file_lock      = None
    slave_state_file_path     = ''      # file used to send signals to slaves
    slave_state_file_lock     = None
    args_file_path            = ''      # file used to send args to slaves to be passed to ants
    ants_registration_command = 'antsRegistrationSyNQuick.sh'

    def define_parameters(self):
        """
        -f filename for fixed image. All other files in the input 
        directory will be interpreted as moving images.

        -s set "speed"
           set to fast to run antsRegistrationSyNQuick.sh
           set to slow to run antsRegistrationSyN.sh

        Define more parameters here as needed.
        """
        self.add_argument('-f', dest='fixed', type=str, optional=False,
                          help='The filename for the fixed image.')
        self.add_argument('-s', dest='speed', type=str, optional=True, 
                          default='antsRegistrationSyNQuick.sh',
                          help='Set to "fast" or "slow" depending on speed and quality desired.')

    def get_worker_number(self):
        """
        Get the next available worker number.
        worker numbers range from 0 to NUMBER_OF_WORKERS-1.
        Create file 'worker_num_sync' in tmp_path, which is used
        to synchronize worker number assignment among available workeres.
        Return the worker number.
        """
        NUMBER_OF_WORKERS = int(os.environ['NUMBER_OF_WORKERS'])
        worker_num = 0
        with self.worker_num_file_lock.acquire():
            try:
                with open(self.worker_num_file_path,'x') as worker_num_file:
                    # Current worker is assigned 0, next worker is assigned 1.
                    worker_num_file.write('1') 
                    worker_num_file.close()
                    worker_num = 0
                    print("PLUGIN DEBUG MSG: {} not found. Assuming that I'm first, \
                           so I'm assigning myself #0 and declaring \
                           my self as master.".format(self.worker_num_file_path))
            except FileExistsError:
                with open(self.worker_num_file_path,'r+') as worker_num_file:
                    # Read worker number and overwrite with next worker number
                    worker_num = int(worker_num_file.read().strip())
                    worker_num_file.seek(0)
                    worker_num_file.write(str(worker_num + 1))
                    worker_num_file.truncate()
                    worker_num_file.close()
        # Check that worker_num is less than number of workeres
        if worker_num >= NUMBER_OF_WORKERS:
            raise ValueError('PLUGIN ERROR MSG: Invalid worker number assigned.\
                              Check worker_num_sync in shared directory.')
        # Wait for all workeres to get their worker number
        last_worker_num = worker_num
        start_time = time.time()
        while last_worker_num < NUMBER_OF_WORKERS:
            with open(self.worker_num_file_path,'r') as worker_num_file:
                last_worker_num = int(worker_num_file.read().strip())
                worker_num_file.close()
            time.sleep(1)
            print("PLUGIN DEBUG MSG: waiting for every one to get proces number. I am #{}, \
                   next worker will be assigned #{}".format(worker_num,last_worker_num))
            if (time.time() - start_time) > 60:
                raise RuntimeError('PLUGIN ERROR MSG: Timed out waiting \
                                    for other instances to get worker number.')
        print('PLUGIN ERROR MSG: Assigned worker Number {}.'.format(worker_num))
        return worker_num

    def exit_worker(self):
        """
        Exit the current worker. Decrement the worker_num_sync file.
        If the worker is the last to leave, then remove tmp directory.
        """
        print("PLUGIN DEBUG MSG: exiting ... ")
        with self.worker_num_file_lock.acquire():
            with open(self.worker_num_file_path,'r+') as worker_num_file:
                # Read worker number and overwrite with decremented worker number
                worker_num = int(worker_num_file.read().strip())
                worker_num_file.seek(0)
                worker_num_file.write(str(worker_num - 1))
                worker_num_file.truncate()
                worker_num_file.close()
        if worker_num == 1:
            subprocess.run(['rm','-rf',self.tmp_path])
        sys.exit()

    def linear_ants_registration_command_wrapper(self,args):
        """
        Run ants registration command (Rigid and Affine stages only).
        Output will be named <name_wo_ext>Warped.nii.gz
      
        Prerequisites:
         * args.keys() = [fixed_image_name,moving_image_name,out_path,name_wo_ext,total_threads]
         * be master worker
        """
        print("PLUGIN DEBUG MSG: Starting linear ants registration stages ... ")
        os.environ['ITK_NUMBER_OF_WORKERS'] = args["number_of_workers"]
        # Ants Registration Call 
        self.run_bash_command_wrapper(self.ants_registration_command +
                                      ' -d 3 -f {} -m {} -o {}/{} -n {} -t a'
                                      .format(args["fixed_image_name"],
                                              args["moving_image_name"],
                                              args["out_path"],
                                              args["name_wo_ext"],
                                              args["total_threads"]))
        # Remove extra outputs
        files_to_be_removed = [args["out_path"] + '/' + args["name_wo_ext"] + 'InverseWarped.nii.gz',
                               args["out_path"] + '/' + args["name_wo_ext"] + '0GenericAffine.mat']
        for filename in files_to_be_removed:
            try:
                os.remove(filename)
            except FileNotFoundError:
                pass

    def syn_ants_registration_command_wrapper(self,args):
        """
        Run ants registration command (SyN stage only).
        Output will be named <name_wo_ext>Warped.nii.gz

        Prerequisites for slave worker:
         * args.keys() = []
        Prerequisites for master worker:
         * args.keys() = [fixed_image_name,moving_image_name,out_path,name_wo_ext,total_threads]
        """
        print("PLUGIN DEBUG MSG: Starting SyN ants registration stage ... ")
        if len(args) == 0:
            # Then this is a slave worker, need to read args from file first.
            with open(self.args_file_path,'r') as args_file:
                args_list = args_file.read().strip('\n').split('\n')
                args["fixed_image_name"]  = args_list[0]
                args["moving_image_name"] = args_list[1]
                args["out_path"]          = args_list[2]
                args["name_wo_ext"]       = args_list[3]
                args["total_threads"]     = args_list[4]
                args_file.close()

        # Ants Registration Call 
        self.run_bash_command_wrapper(self.ants_registration_command +
                                      ' -d 3 -f {} -m {} -o {}/{} -n {} -t so' 
                                      .format(args["fixed_image_name"],
                                              args["moving_image_name"],
                                              args["out_path"],
                                              args["name_wo_ext"],
                                              args["total_threads"]))
        # Remove extra outputs
        files_to_be_removed = [args["out_path"] + '/' + args["name_wo_ext"] + '1InverseWarp.nii.gz',
                               args["out_path"] + '/' + args["name_wo_ext"] + '1Warp.nii.gz',
                               args["out_path"] + '/' + args["name_wo_ext"] + 'InverseWarped.nii.gz',
                               args["out_path"] + '/' + args["name_wo_ext"] + '0GenericAffine.mat']
        for filename in files_to_be_removed:
            try:
                os.remove(filename)
            except FileNotFoundError:
                pass

    def get_state(self):
        """
        Return the state of slave workeres (as given by master), either IDLE, EXIT, or START.
        All slaves read from the same state file 'slave_state' in tmp_path.
        This function should only be called by slave workeres.
        """
        with self.slave_state_file_lock.acquire():
            with open(self.slave_state_file_path,'r') as slave_state_file:
                state = int(slave_state_file.read().strip())
                slave_state_file.close()
        return state

    def write_state(self, state):
        """
        Write the state to the slave sate file in tmp_path 'slave_state'.
        State must be EXIT, START, or IDLE.
        Should only be called by master worker.
        """
        with self.slave_state_file_lock.acquire():
            with open(self.slave_state_file_path,'w') as slave_state_file:
                slave_state_file.write(str(state))
                slave_state_file.close()

    def run_parallel_ants_registration_slave(self):
        """
        Must be called by slave workeres.
        This function never returns, it exits python normally when an EXIT signal
        is sent by the master worker.
        The slave waits for START signal from master before running ants registration.
        """
        state = self.IDLE
        while state != self.EXIT:
            try:
            	state = self.get_state()
            except FileNotFoundError:
                # Most likely because master hasn't created slave_state file yet
                pass
            if state == self.START:
                self.syn_ants_registration_command_wrapper({})
                # Wait for IDLE signal from master 
                while state != self.IDLE:
                    state = self.get_state()
                    time.sleep(1)
            print("PLUGIN DEBUG MSG: Waiting for master ... ")
            time.sleep(1)
        self.exit_worker()

    def write_args_to_file(self, args):
        """
        Should only be called by master worker.
        This method writes the arguments that need to be passed to the ants registration
        command to file names 'args_file' so that slave workeres know what args to pass.
        """
        with open(self.args_file_path,'w') as args_file:
            args_file.write(args["fixed_image_name"])
            args_file.write('\n')
            args_file.write(args["moving_image_name"])
            args_file.write('\n')
            args_file.write(args["out_path"])
            args_file.write('\n')
            args_file.write(args["name_wo_ext"])
            args_file.write('\n')
            args_file.write(args["total_threads"])
            args_file.write('\n')
            args_file.close()
        
    def run_parallel_ants_registration_master(self, linear_ants_args, syn_ants_args):
        """
        Should only be called by master worker. 
        This is executed when master worker is ready to run ants registration.
        """
        # Run Linear stage with single worker
        self.linear_ants_registration_command_wrapper(linear_ants_args)
        # Run SyN stage with all workers
        os.environ['ITK_NUMBER_OF_WORKERS'] = syn_ants_args["number_of_workers"]
        self.write_args_to_file(syn_ants_args)
        self.write_state(self.START)
        self.syn_ants_registration_command_wrapper(syn_ants_args)
        self.write_state(self.IDLE)

    def run_parallel_ants_registration_master_wrapper(self,
                                                      fixed_image_name,
                                                      moving_image_name,
                                                      out_path):
        """
        Wrapper function that does preprocessing and postprocessing before 
        ants registration proper.
        ust be master to call.
        """
        name_wo_ext = moving_image_name.split('/')[-1].split('.')[0]
        self.configure_env_for_multi_threaded_execution()

        linear_ants_args = {
                            "fixed_image_name":   fixed_image_name,
                            "moving_image_name":  moving_image_name,
                            "out_path":           out_path,
                            "name_wo_ext":        name_wo_ext,
                            "number_of_workers":  '1',
                            "total_threads":      os.environ['ITK_THREADS_PER_WORKER']
                           }
        # moving image for syn registration is output of linear registration
        syn_ants_args    = {
                            "fixed_image_name":   fixed_image_name,
                            "moving_image_name":  out_path + '/' + name_wo_ext + 'Warped.nii.gz',
                            "out_path":           out_path,
                            "name_wo_ext":        name_wo_ext,
                            "number_of_workers":  os.environ['ITK_NUMBER_OF_WORKERS'],
                            "total_threads":      os.environ['ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS']
                           }
        self.run_parallel_ants_registration_master(linear_ants_args, syn_ants_args)
        
        self.make_tiled_mosaic_jpeg_wrapper('{}/{}Warped.nii.gz'.format(out_path, name_wo_ext),
                                            '{}/{}WarpedTiled.jpg'.format(out_path, name_wo_ext),
                                            out_path)
        # Remove extra outputs
        files_to_be_removed = [out_path + '/' + name_wo_ext + 'WarpedTiled.nii']
        for filename in files_to_be_removed:
            try:
                os.remove(filename)
            except FileNotFoundError:
                pass

    def make_tiled_mosaic_jpeg_wrapper(self,in_file_path, out_file_path, tmp_path):
        # Set number of threads to one, since these apps were not proxess parallelized
        self.configure_env_for_single_threaded_execution()
        # Make JPEG image of fixed image
        self.run_bash_command_wrapper('CreateTiledMosaic -i {} -o {}/FixedTiled.nii' 
                                      .format(in_file_path, tmp_path))
        self.run_bash_command_wrapper('ConvertToJpg {}/FixedTiled.nii {}'
                                      .format(tmp_path, out_file_path))
        os.remove(tmp_path + '/FixedTiled.nii')

    def dcm_to_nii_wrapper(self, nii_filename, dcm_input_dir_path):
        """
        self.tmp_path assumed to be output directory.
        """
        self.run_bash_command_wrapper(
                                      'dcm2niix -o {} -f {} {}'
                                      .format(self.tmp_path,
                                              nii_filename,
                                              dcm_input_dir_path)
                                     )

    def run_bash_command_wrapper(self,command_str):
        print("PLUGIN DEBUG MSG: Running bash command: {}".format(command_str))
        #subprocess.run(command_str.split())
        subprocess.call(command_str, shell=True)

    def configure_env_for_single_threaded_execution(self):
        os.environ['ITK_THREADS_PER_WORKER'] = '1'
        os.environ['ITK_NUMBER_OF_WORKERS'] = '1'
        os.environ['ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS'] = '1'

    def configure_env_for_multi_threaded_execution(self):
        threads_per_worker = int(int(os.environ['CPU_LIMIT'].strip('m'))/1000)
        os.environ['ITK_THREADS_PER_WORKER'] = str(threads_per_worker)
        os.environ['ITK_NUMBER_OF_WORKERS'] = os.environ['NUMBER_OF_WORKERS']
        os.environ['ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS'] = \
          str(int(os.environ['NUMBER_OF_WORKERS']) * threads_per_worker)

    def run(self, options):
        """
        Execute default command. 

        Pre-requisites: 
        * Acceptable input file formats: .nii, .nii.gz, or folder of .dcm slices.
        * Three dimenstional
        * All directories are assumed to be a collection of 2D DICOM slices and treated as one volume.
        * NUMBER_OF_WORKERS env variable set to positive number.

        DICOM files are converted to 3D NIFTI volume before image registration.
        files without .dcm extension in DICOM directory are ignored.
        Slices in single DICOM directory must be in same anatomical plane. If a DICOM directory
        contians images from different anatomical plane, then it will be converted to 
        multiple volumes. Only one of these volumes would be registered to fixed image.
        
        Output Specifications:
        * FixedTiled.jpg is tiled representation of the fixed volume.
        * For each moving image, there are two outputs: <prefix>Warped.nii.gz 
          and <prefix>WarpedTiled.jpg. Prefix is name of input image stripped of file extension.

        Make sure output directory is world writable.

		If program crashes, remove <output_dir>/tmp manually.
        """
        if options.fixed == None:
            self.error("A fixed image is required.")
            return
        if options.speed != None:
            if options.speed == 'fast':
                self.ants_registration_command = 'antsRegistrationSyNQuick.sh'
            elif options.speed == 'slow':
                self.ants_registration_command = 'antsRegistrationSyN.sh'

        out_path = options.outputdir
        in_path = options.inputdir

        # Make tmp folder to hold output of DICOM -> NIFTI conversion and
        # interworker communication files and initiate file paths
        self.tmp_path                = out_path + '/tmp'
        self.worker_num_file_path    = self.tmp_path + '/worker_num_sync'
        self.worker_num_file_lock    = FileLock(self.tmp_path + '/worker_num_sync.lock')
        self.slave_state_file_path   = self.tmp_path + '/slave_state'
        self.slave_state_file_lock   = FileLock(self.tmp_path + '/slave_state.lock')
        self.args_file_path          = self.tmp_path + '/args_file'
        try:
            os.mkdir(self.tmp_path)
        except FileExistsError:
            pass

        # Get worker number. worker #0 becomes master.
        master = False;
        worker_num = self.get_worker_number()
        if worker_num == 0:
            master = True
        os.environ['ITK_WORKER_NUMBER'] = str(worker_num)
        os.environ['ITK_BARRIER_FILE_PREFIX'] = self.tmp_path + '/itkbarrier'
        os.environ['ITK_DATA_FILE_PREFIX']    = self.tmp_path + '/itkdata'
        os.environ['ITK_BARRIER_FILES_RESET'] = '1'
        self.configure_env_for_multi_threaded_execution()

        # This is the point where slave workeres wait for master 
        if master:
        	self.write_state(self.IDLE)
        else:
            # Slaves never return from this function.
            self.run_parallel_ants_registration_slave();
        # Master worker must reset itkbarrier files to 0
        for i in range(int(os.environ['NUMBER_OF_WORKERS'])):
            with open(self.tmp_path + '/itkbarrier' + str(i),'wb+') as barrier_file:
                barrier_file.write(b'\0'*8) #unsigned long
                barrier_file.close()

        # Variables below store abosolute file paths
        fixed_image_name = ''           # Filename for fixed image
        moving_image_list = []          # List of filenames for moving images           
        
        if os.path.isfile(in_path+'/'+options.fixed):
            fixed_image_name = in_path + '/' + options.fixed
        else:
            # Fixed image is a directory. Assume to contain .dcm files
            fixed_image_name = 'fixed_image'
            self.dcm_to_nii_wrapper(fixed_image_name, in_path + '/' + options.fixed)
            fixed_image_name = self.tmp_path + '/fixed_image.nii'

        for name in os.listdir(in_path):
            if name == options.fixed or name == fixed_image_name: 
                continue
            if not os.path.isfile(in_path + '/' + name):
                # Assume to be directory full of .dcm slices
                self.dcm_to_nii_wrapper(name, in_path + '/' + name)
                # dcm2niix might output multiple nifti files, but we'll assume there is only one
                moving_image_list.append(self.tmp_path + '/' + name+'.nii')

            else:
                filename_split = name.strip().split('.')
                if len(filename_split) > 1 and filename_split[-1] == 'nii':   # .nii extension
                    moving_image_list.append(in_path + '/' + name)
                elif len(filename_split) > 2 and filename_split[-2] == 'nii': # .nii.gz extension
                    moving_image_list.append(in_path + '/' + name)

        self.make_tiled_mosaic_jpeg_wrapper(fixed_image_name,
                                            '{}/FixedTiled.jpg'.format(out_path),
                                            out_path)
        # Run ANTS registration on each of the moving images and create JPEG Tiled image
        for moving_image_name in moving_image_list:
            print("PLUGIN DEBUG MSG: Registering {} to {} ... "
                  .format(moving_image_name, fixed_image_name))
            self.run_parallel_ants_registration_master_wrapper(fixed_image_name,
                                                               moving_image_name,
                                                               out_path)
        self.write_state(self.EXIT)  # Terminate slave workeres
        self.exit_worker()

# ENTRYPOINT
if __name__ == "__main__":
    app = AntsReg()
    app.launch()

