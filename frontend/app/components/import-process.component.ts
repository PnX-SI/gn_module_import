import { Component, OnInit } from '@angular/core';
import { Router } from "@angular/router";
import { MatStepperModule } from '@angular/material/stepper';


@Component({
  selector: 'pnx-import-process',
  styleUrls: ['import-process.component.scss'],
  templateUrl: 'import-process.component.html'
})


export class ImportProcessComponent implements OnInit {

  selectedFile: File = null;

  constructor(
    private _router: Router
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

}

