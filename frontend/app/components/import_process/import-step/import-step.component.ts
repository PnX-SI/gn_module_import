import { Component, OnInit, OnChanges, Input } from '@angular/core';
import { StepsService } from '../steps.service';
import { DataService } from '../../../services/data.service';
import { ToastrService } from 'ngx-toastr';

@Component({
	selector: 'import-step',
	styleUrls: [ 'import-step.component.scss' ],
	templateUrl: 'import-step.component.html'
})
export class ImportStepComponent implements OnInit, OnChanges {

	public isCollapsed = false;
    //@Input() contentMappingInfo: any;
    isUserError: boolean = false;
    contentMapRes: any;


	constructor(
        private stepService: StepsService, 
        private _ds: DataService,
        private toastr: ToastrService
        ) {}


	ngOnInit() {
	}


	onStepBack() {
		this.stepService.previousStep();
    }

    onImport() {
        console.log('my import');
    }
    
}