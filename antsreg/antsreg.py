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
 
    # Signals for interprocess communication
    IDLE  = 0
    START = 1
    EXIT  = 2

    # Files for interprocess communication 
    tmp_path                = ''
    proc_num_file_path      = ''
    proc_num_file_lock      = None
    slave_state_file_path   = ''
    slave_state_file_lock   = None
    args_file_path          = ''

    def define_parameters(self):
        """
        One parameter currently defined.

        -f filename for fixed image. All other files in the input 
        directory will be interpreted as moving images.

        Define more parameters here as needed.
        """
        self.add_argument('-f', dest='fixed', type=str, optional=False,
                          help='The filename for the fixed image.')

    def get_process_number(self):
        """
        Get the next available process number.
        Process numbers range from 0 to NUMBER_OF_WORKERS-1.
        Create file 'proc_num_sync' in tmp_path, which is used
        to synchronize process number assignment among available processes.
        Return the process number.
        """
        NUMBER_OF_WORKERS = int(os.environ['NUMBER_OF_WORKERS'])
        proc_num = 0
        with self.proc_num_file_lock.acquire():
            try:
                with open(self.proc_num_file_path,'x') as proc_num_file:
                    # Current process is assigned 0, next process is assigned 1.
                    proc_num_file.write('1') 
                    proc_num_file.close()
                    proc_num = 0
            except FileExistsError:
                with open(self.proc_num_file_path,'r+') as proc_num_file:
                    # Read process number and overwrite with next process number
                    proc_num = int(proc_num_file.read().strip())
                    proc_num_file.seek(0)
                    proc_num_file.write(str(proc_num + 1))
                    proc_num_file.truncate()
                    proc_num_file.close()
        # Check that proc_num is less than number of processes
        if proc_num >= NUMBER_OF_WORKERS:
            raise ValueError('Invalid process number assigned.\
                              Check proc_num_sync in shared directory.')
        # Wait for all processes to get their process number
        last_proc_num = proc_num
        start_time = time.time()
        while last_proc_num < NUMBER_OF_WORKERS:
            with open(self.proc_num_file_path,'r') as proc_num_file:
                last_proc_num = int(proc_num_file.read().strip())
                proc_num_file.close()
            time.sleep(1)
            if (time.time() - start_time) > 60:
                raise RuntimeError('Timed out waiting for other instances to get process number.')
        print('Assigned Process Number {}.'.format(proc_num))
        return proc_num

    def exit_process(self):
        """
        Exit the current process. Decrement the proc_num_sync file.
        If the process is the last to leave, then remove tmp directory.
        """
        with self.proc_num_file_lock.acquire():
            with open(self.proc_num_file_path,'r+') as proc_num_file:
                # Read process number and overwrite with decremented process number
                proc_num = int(proc_num_file.read().strip())
                proc_num_file.seek(0)
                proc_num_file.write(str(proc_num - 1))
                proc_num_file.truncate()
                proc_num_file.close()
        if proc_num == 1:
            subprocess.run(['rm','-rf',self.tmp_path])
        sys.exit()

    def ants_registration_command_wrapper(self,args):
        """
        Run ants registration command.
        args = [fixed_image_name,moving_image_name,out_path,name_wo_ext]
        """
        if len(args) == 0:
            # Then this is a slave process, need to read args from file first.
            with open(self.args_file_path,'r') as args_file:
                args = args_file.read().strip('\n').split('\n')
                args_file.close()
        # Ants Registration Call 
        subprocess.run('antsRegistrationSyNQuick.sh -d 3 -f {} \
                        -m {} -o {}/{} -n 4' 
                       .format(args[0],args[1],args[2],args[3]).split())
        # Output will be named <name_wo_ext>Warped.nii.gz

    def get_state(self):
        """
        Return the state of slave processes (as given by master), either IDLE, EXIT, or START.
        All slaves read from the same state file 'slave_state' in tmp_path.
        This function should only be called by slave processes.
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
        Should only be called by master process.
        """
        with self.slave_state_file_lock.acquire():
            with open(self.slave_state_file_path,'w') as slave_state_file:
                slave_state_file.write(str(state))
                slave_state_file.close()

    def run_parallel_ants_registration_slave(self):
        """
        Must be called by slave processes.
        This function never returns, it exits python normally when an EXIT signal
        is sent by the master process.
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
                self.ants_registration_command_wrapper([])
                # Wait for IDLE signal from master 
                while state != self.IDLE:
                    state = self.get_state()
                    time.sleep(1)
            time.sleep(1)
        self.exit_process()

    def write_args_to_file(self, args):
        """
        Should only be called by master process.
        This method writes the arguments that need to be passed to the ants registration
        command to file names 'args_file' so that slave processes know what args to pass.
        """
        with open(self.args_file_path,'w') as args_file:
            for arg in args:
                args_file.write(arg)
                args_file.write('\n')
            args_file.close()
        
    def run_parallel_ants_registration_master(self, args):
        """
        Should only be called by master process. 
        This is executed when master process is ready to run ants registration.
        """
        self.write_args_to_file(args)
        self.write_state(self.START)
        self.ants_registration_command_wrapper(args)
        self.write_state(self.IDLE)

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
            
        out_path = options.outputdir
        in_path = options.inputdir

        # Make tmp folder to hold output of DICOM -> NIFTI conversion and
        # interprocess communication files and initiate file paths
        self.tmp_path                = out_path + '/tmp'   
        self.proc_num_file_path      = self.tmp_path + '/proc_num_sync'
        self.proc_num_file_lock      = FileLock(self.tmp_path + '/proc_num_sync.lock')
        self.slave_state_file_path   = self.tmp_path + '/slave_state'
        self.slave_state_file_lock   = FileLock(self.tmp_path + '/slave_state.lock')
        self.args_file_path          = self.tmp_path + '/args_file'
        try:
            os.mkdir(self.tmp_path)
        except FileExistsError:
            pass

        # Get process number. Process #0 becomes master.
        master = False;
        proc_num = self.get_process_number()
        if proc_num == 0:
            master = True
        os.environ['ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS'] = os.environ['NUMBER_OF_WORKERS']
        os.environ['ITK_PROCESS_NUMBER']      = str(proc_num)
        os.environ['ITK_BARRIER_FILE_PREFIX'] = self.tmp_path + '/itkbarrier'
        os.environ['ITK_DATA_FILE_PREFIX']    = self.tmp_path + '/itkdata'
        os.environ['ITK_BARRIER_FILES_RESET'] = '1'

        # This is the point where slave processes wait for master 
        if master:
        	self.write_state(self.IDLE)
        else:
            # Slaves never return from this function.
            self.run_parallel_ants_registration_slave();
        # Master process must reset itkbarrier files to 0
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
            subprocess.run('dcm2niix -o {} -f {} {}/{}'
                           .format(self.tmp_path,fixed_image_name,in_path,options.fixed).split())
            fixed_image_name = self.tmp_path + '/fixed_image.nii'
        
        for name in os.listdir(in_path):
            if name == options.fixed or name == fixed_image_name: 
                continue
            if not os.path.isfile(in_path + '/' + name):
                # Assume to be directory full of .dcm slices
                subprocess.run('dcm2niix -o {} -f {} {}/{}'
                               .format(self.tmp_path,name,in_path,name).split())
                # dcm2niix might output multiple nifti files, but we'll assume there is only one
                moving_image_list.append(self.tmp_path + '/' + name+'.nii')

            else:
                filename_split = name.strip().split('.')
                if len(filename_split) > 1 and filename_split[-1] == 'nii':   # .nii extension
                    moving_image_list.append(in_path + '/' + name)
                elif len(filename_split) > 2 and filename_split[-2] == 'nii': # .nii.gz extension
                    moving_image_list.append(in_path + '/' + name)

        # Set Enviornment variable for single process exectution
        os.environ['ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS'] = '1'
        # Make JPEG image of fixed image
        subprocess.run('CreateTiledMosaic -i {} -o {}/FixedTiled.nii' 
                       .format(fixed_image_name, out_path)
                       .split())
        subprocess.run('ConvertToJpg {}/FixedTiled.nii {}/FixedTiled.jpg' 
                       .format(out_path, out_path)
                       .split())
        os.remove(out_path + '/FixedTiled.nii')
     
        # Run ANTS registration on each of the moving images and create JPEG Tiled image
        for moving_image_name in moving_image_list:
            name_wo_ext = moving_image_name.split('/')[-1].split('.')[0]

            # Set Enviornment variable for multiple process exectution
            os.environ['ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS'] = os.environ['NUMBER_OF_WORKERS']
            args = [fixed_image_name,moving_image_name,out_path,name_wo_ext]
            self.run_parallel_ants_registration_master(args)
            
            # Set Enviornment variable for single process exectution
            os.environ['ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS'] = '1'
            # Make Tiled Mosaic JPEG
            subprocess.run('CreateTiledMosaic -i {}/{}Warped.nii.gz -o {}/{}WarpedTiled.nii' 
                           .format(out_path, name_wo_ext, 
                                   out_path, name_wo_ext)
                           .split())
            subprocess.run('ConvertToJpg {}/{}WarpedTiled.nii {}/{}WarpedTiled.jpg' 
                           .format(out_path, name_wo_ext,
                                   out_path, name_wo_ext)
                           .split())
            
            # Remove extra outputs
            files_to_be_removed = [out_path + '/' + name_wo_ext + '1InverseWarp.nii.gz',
                                   out_path + '/' + name_wo_ext + '1Warp.nii.gz',
                                   out_path + '/' + name_wo_ext + 'InverseWarped.nii.gz',
                                   out_path + '/' + name_wo_ext + '0GenericAffine.mat',
                                   out_path + '/' + name_wo_ext + 'WarpedTiled.nii']
            for filename in files_to_be_removed:
                try:
                    os.remove(filename)
                except FileNotFoundError:
                    pass
        self.write_state(self.EXIT)  # Terminate slave processes
        self.exit_process()

# ENTRYPOINT
if __name__ == "__main__":
    app = AntsReg()
    app.launch()

