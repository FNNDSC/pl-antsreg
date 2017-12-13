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
import subprocess

# import the Chris app superclass
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

    # Fill out this with key-value output descriptive info (such as an output file path
    # relative to the output dir) that you want to save to the output meta file when
    # called with the --saveoutputmeta flag
    OUTPUT_META_DICT = {}
 
    def define_parameters(self):
        """
        One parameter currently defined.

        -f filename for fixed image. All other files in the input 
        directory will be interpreted as moving images.

        Define more parameters here as needed.
        """
        self.add_argument('-f', dest='fixed', type=str, optional=False,
                          help='The filename for the fixed image.')

    def run(self, options):
        """
        Execute default command. 

        Pre-requisites: 
        * Acceptable input file formats: .nii, .nii.gz, or folder of .dcm slices.
        * Three dimenstional
        * All directories are assumed to be a collection of 2D DICOM slices and treated as one volume.
        
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
        """

        if options.fixed == None:
            self.error("A fixed image is required.")
            return
            
        out_path = options.outputdir
        in_path = options.inputdir
        tmp_path = out_path + '/tmp'    # Make tmp folder to hold output of DICOM -> NIFTI conversion
        try:
            os.mkdir(tmp_path)
        except FileExistsError:
            pass

        # varaibles below store abosolute file paths
        fixed_image_name = ''           # Filename for fixed image
        moving_image_list = []          # List of filenames for moving images           
        
        if os.path.isfile(in_path+'/'+options.fixed):
            fixed_image_name = in_path + '/' + options.fixed
        else:
            # Fixed image is a directory. Assume to contain .dcm files
            fixed_image_name = 'fixed_image'
            subprocess.run('dcm2niix -o {} -f {} {}/{}'
                           .format(tmp_path,fixed_image_name,in_path,options.fixed).split())
            fixed_image_name = tmp_path + '/' + 'fixed_image.nii'
        
        for name in os.listdir(in_path):
            if name == options.fixed or name == fixed_image_name: 
                continue
            if not os.path.isfile(in_path + '/' + name):
                # Assume to be directory full of .dcm slices
                subprocess.run('dcm2niix -o {} -f {} {}/{}'
                               .format(tmp_path,name,in_path,name).split())
                # dcm2niix might output multiple nifti files, but we'll assume there is only one
                moving_image_list.append(tmp_path + '/' + name+'.nii')

            else:
                filename_split = name.strip().split('.')
                if len(filename_split) > 1 and filename_split[-1] == 'nii':   # .nii extension
                    moving_image_list.append(in_path + '/' + name)
                elif len(filename_split) > 2 and filename_split[-2] == 'nii': # .nii.gz extension
                    moving_image_list.append(in_path + '/' + name)
            
        # Make JPEG image of fixed image
        subprocess.run('CreateTiledMosaic -i {} -o {}/FixedTiled.nii' 
                       .format(fixed_image_name, out_path)
                       .split())
        subprocess.run('ConvertToJpg {}/FixedTiled.nii {}/FixedTiled.jpg' 
                       .format(out_path, out_path)
                       .split())
        os.remove(out_path + '/FixedTiled.nii')
     
        # Run ANTS registration on each of the mocing images and create JPEG Tiled image
        for moving_image_name in moving_image_list:
            # Change antsRegistrationSyNQuick.sh to antsRegistrationSyN.sh when not testing
            name_wo_ext = moving_image_name.split('/')[-1].split('.')[0]

            # Ants Registration Call
            subprocess.run('antsRegistrationSyN.sh -d 3 -f {} \
                            -m {} -o {}/{}' 
                           .format(fixed_image_name,
                                   moving_image_name, out_path,
                                   name_wo_ext)
                           .split())
            # output will be named <name_wo_ext>Warped.nii.gz
            
            # Make Tiled Mosaic JPEG
            subprocess.run('CreateTiledMosaic -i {}/{}Warped.nii.gz -o {}/{}WarpedTiled.nii' 
                           .format(out_path, name_wo_ext, 
                                   out_path, name_wo_ext)
                           .split())
            subprocess.run('ConvertToJpg {}/{}WarpedTiled.nii {}/{}WarpedTiled.jpg' 
                           .format(out_path, name_wo_ext,
                                   out_path, name_wo_ext)
                           .split())
            
            # remove extra outputs
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
        subprocess.run(['rm','-rf',tmp_path])

# ENTRYPOINT
if __name__ == "__main__":
    app = AntsReg()
    app.launch()







