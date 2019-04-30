import { Component, OnInit, ViewChild } from '@angular/core';
import { Router } from "@angular/router";
import { MatStepperModule } from '@angular/material/stepper';
import { DataService } from '../services/data.service';
import { ToastrService } from 'ngx-toastr';
import { ModuleConfig } from '../module.config';
import { MatButtonModule } from '@angular/material/button';
import { STEPPER_GLOBAL_OPTIONS } from '@angular/cdk/stepper';


@Component({
  selector: 'pnx-import-process',
  styleUrls: ['import-process.component.scss'],
  templateUrl: 'import-process.component.html',
  providers: [{
    provide: STEPPER_GLOBAL_OPTIONS, useValue: {displayDefaultIndicatorType: false}
  }]
})


export class ImportProcessComponent implements OnInit {

  selectedFile: File = null;
  public uploadResponse: JSON;
  public IMPORT_CONFIG = ModuleConfig;
  public isFileSelected : Boolean = false; // used for disable valid button
  //step1Completed = false;
  //isLinear = true;

  @ViewChild('stepper') stepper: MatStepperModule;

  constructor(
    private _router: Router, 
    public _ds: DataService,
    private toastr: ToastrService
  ) {}


  ngOnInit() {
  }

  resetStepper(stepper: MatStepperModule){
    stepper.selectedIndex = 0;
  }

  onFileSelected(event) {
    this.selectedFile = <File>event.target.files[0];
    this.isFileSelected = true;
    console.log('le nom du fichier uploadé par l\'utilisateur est = ' + this.selectedFile.name);
  }

  onUpload() {
    this._ds.postUserFile(this.selectedFile).subscribe(
      res => {
        this.uploadResponse = res as JSON;
    },
      error => {
        if (error.statusText === 'Unknown Error') {
          // show error message if no connexion
          this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
        } else {
          // show error message if other server error
          this.toastr.error(error.error);
        }
    },
      () => {
        //this.isLinear = false;
        console.log(this.uploadResponse);

        //this.complete();
        // vérifier validité du csv format : en l'ouvrant (csv.read)?
        // promesse pour bloquer front en attendant que ce soit fini
      }
    );
  } 

  onImportList() {
    this._router.navigate([`${this.IMPORT_CONFIG.MODULE_URL}`]);
  }

  onCancel() {
    this.onImportList();
    // effacer le fichier dans uploads (attention penser à gérer le fait que 2 utilisateurs puissent avoir le même nom de fichier?)
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

