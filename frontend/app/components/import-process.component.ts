import { Component, OnInit } from '@angular/core';
import { Router } from "@angular/router";
import { MatStepperModule } from '@angular/material/stepper';
import { DataService } from '../services/data.service';
import { ToastrService } from 'ngx-toastr';
import { ModuleConfig } from '../module.config';
import { MatButtonModule } from '@angular/material/button';


@Component({
  selector: 'pnx-import-process',
  styleUrls: ['import-process.component.scss'],
  templateUrl: 'import-process.component.html'
})


export class ImportProcessComponent implements OnInit {

  selectedFile: File = null;
  public uploadServerMsg: JSON;
  public IMPORT_CONFIG = ModuleConfig;

  constructor(
    private _router: Router, 
    public _ds: DataService,
    private toastr: ToastrService
  ) {}


  ngOnInit() {
  }

  resetStepper(stepper: MatStepper){
    stepper.selectedIndex = 0;
  }

  onFileSelected(event) {
    this.selectedFile = <File>event.target.files[0];
    console.log('le nom du fichier uploadé par l\'utilisateur est = ' + this.selectedFile.name);
  }

  onUpload() {
    this._ds.postUserFile(this.selectedFile).subscribe(
      res => {
        this.uploadServerMsg = res as JSON;
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
        console.log(this.uploadServerMsg);
        // vérifier validité du csv format : en l'ouvrant (csv.read)?
        // promesse pour bloquer front en attendant que ce soit fini
      }
    );
  } 

  onImportList() {
    this._router.navigate([`${this.IMPORT_CONFIG.MODULE_URL}`]);
  }

}

