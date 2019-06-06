import { Component, OnInit, ViewChild } from '@angular/core';
import { Router,ActivatedRoute } from "@angular/router";
import { MatStepperModule } from '@angular/material/stepper';
import { DataService } from '../services/data.service';
import { ToastrService } from 'ngx-toastr';
import { ModuleConfig } from '../module.config';
import { MatButtonModule } from '@angular/material/button';
import { STEPPER_GLOBAL_OPTIONS } from '@angular/cdk/stepper';
import { FormControl, FormGroup, FormBuilder, Validators } from '@angular/forms';


@Component({
  selector: 'pnx-import-process',
  styleUrls: ['import-process.component.scss'],
  templateUrl: 'import-process.component.html'
})


export class ImportProcessComponent implements OnInit {

  public fileName;
  public uploadResponse: JSON;
  public isUploaded: Boolean = false;
  public isUploading: Boolean = false;
  public IMPORT_CONFIG = ModuleConfig;
  public uploadForm: FormGroup;
  public importId;
  public cancelResponse;
  public synColumnNames;
  public mappingResponse: JSON;
  public syntheseForm: FormGroup;

  @ViewChild('stepper') stepper: MatStepperModule;

  constructor(
    private _router: Router, 
    private _activatedRoute: ActivatedRoute,
    public _ds: DataService,
    private toastr: ToastrService,
    private _fb: FormBuilder
  ) {

    this._activatedRoute.params.subscribe(
      //params => console.log(params)
      );

    this.uploadForm = this._fb.group({
      file: [null, Validators.required],
      encodage: [null, Validators.required],
      srid: [null, Validators.required],
      separator: [null, Validators.required]
    });

    this.syntheseForm = this._fb.group({
      /*
      nomCite: [null, Validators.required],
      date_min: [null, Validators.required],
      date_max: [null, Validators.required]
      */
    });
  }


  ngOnInit() {
    //console.log(this._activatedRoute.params._value);
    this.importId = 'undefined';
    this.getSynColumnNames();
  }


  resetStepper(stepper: MatStepperModule){
    stepper.selectedIndex = 0;
  }


  onFileSelected(event) {
    console.log(event);
    this.uploadForm.patchValue({
      file: <File>event.target.files[0]
    });
    this.isFileSelected = true;
    this.fileName = this.uploadForm.get('file').value.name;
    console.log('nom du fichier = ' + this.fileName);
  }


  cancelImport() {
    console.log(this.importId);
    this._ds.cancelImport(this.importId).subscribe(
      res => {
        this.cancelResponse = res as JSON;
    },
      error => {
        if (error.statusText === 'Unknown Error') {
          // show error message if no connexion
          this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
        } else {
          if (error.status = 400){
            this._router.navigate([`${this.IMPORT_CONFIG.MODULE_URL}`]);
          }
          // show error message if other server error
          this.toastr.error(error.error);
        }
    },
      () => {
        console.log(this.cancelResponse);
        this.onImportList();
      }
    );       
  }


  onUpload(value) {
    this.isUploading = true;
    this._ds.postUserFile(value,this._activatedRoute.params._value['datasetId'],this.importId).subscribe(
      res => {
        this.uploadResponse = res as JSON;
    },
      error => {
        if (error.statusText === 'Unknown Error') {
          // show error message if no connexion
          this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
        } else {
          if (error.status == 400) {
            this.toastr.error(
              "ERREUR : " +
              error.error['errorInterpretation'] + " : " 
              error.error['errorMessage'] + 
              error.error['errorContext'],
              {timeOut: 100000});
          }
          if (error.status == 500) {
            // show error message if other server error
            this.toastr.error(error.error);
          }
          this.isUploading = false;
        }
    },
      () => {
        //this.isLinear = false;
        console.log(this.uploadResponse);
        this.importId = this.uploadResponse.importId;
        this.columns = this.uploadResponse.columns;
        console.log(this.columns);
        console.log(this.importId);
        this.isUploaded = true;
        this.stepper.next();
        this.isUploading = false;

        // promesse pour bloquer front en attendant que ce soit fini
      }
    );
  } 


  onMapping(value) {
    this._ds.postMapping(value, this.importId).subscribe(
      res => {
        this.mappingResponse = res as JSON;
    },
      error => {
        if (error.statusText === 'Unknown Error') {
          // show error message if no connexion
          this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
        } else {
            // show error message if other server error
            this.toastr.error(error.error.message);
        }
    },
      () => {
        console.log(this.mappingResponse);
      }
    );
  }


  onImportList() {
    // effacer le fichier dans uploads (attention penser à gérer le fait que 2 utilisateurs puissent avoir le même nom de fichier?)
    this._router.navigate([`${this.IMPORT_CONFIG.MODULE_URL}`]);
  }


  getSynColumnNames() {
    this._ds.getSynColumnNames().subscribe(
      res => {
        this.synColumnNames = res as JSON;
    },
      error => {
        if (error.statusText === 'Unknown Error') {
          // show error message if no connexion
          this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
        } else {
          // show error message if other server error
          this.toastr.error(error.error.message);
        }
    },
      () => {
        //this.isLinear = false;
        console.log(this.synColumnNames);
        for (let col of this.synColumnNames){
          this.syntheseForm.addControl(col, new FormControl('', Validators.required));
        }
        console.log(this.syntheseForm);
      }
    );
}

  complete() {
    /*
    console.log(this.stepper);
    console.log(this.stepper.selected.completed);
    //this.stepper.selected.editable = false;
    this.stepper.completed = true;
    if (this.stepper.completed = true) {
      this.stepper.next();
    }
    */
  }

}

