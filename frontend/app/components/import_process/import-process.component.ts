import { Component, OnInit, ViewChild, ChangeDetectorRef } from '@angular/core';
import { Router } from '@angular/router';
import { DataService } from '../../services/data.service';
import { ToastrService } from 'ngx-toastr';
import { ModuleConfig } from '../../module.config';
import { MatStepper } from '@angular/material';
import { FormGroup, FormBuilder, Validators } from '@angular/forms';
import { StepsService } from './steps.service';

@Component({
	selector: 'pnx-import-process',
	styleUrls: [ 'import-process.component.scss' ],
	templateUrl: 'import-process.component.html'
})
export class ImportProcessComponent implements OnInit {
	public step1Control: FormGroup;
	public step2Control: FormGroup;
	public step3Control: FormGroup;
	public step4Control: FormGroup;
	public initForm: FormGroup;
	public srid: any;
	public columns;
	public IMPORT_CONFIG = ModuleConfig;
	isLinear: boolean = true;
	contentMappingInfo: any;
	table_name: any;
	selected_columns: any;
	added_columns: any;
	importId: any;

	@ViewChild('stepper') stepper: MatStepper;
	stepId: any;

	constructor(
		private _router: Router,
		private _fb: FormBuilder,
		private _ds: DataService,
		private toastr: ToastrService,
		private stepService: StepsService,
		private cd: ChangeDetectorRef
	) {
		this.initForm = this._fb.group({
			fromStep: [ null, Validators.required ]
		});
		this.step1Control = this.initForm;
		this.step2Control = this.initForm;
		this.step3Control = this.initForm;
		this.step4Control = this.initForm;
		
	}

	ngOnInit() {}

	ngAfterViewInit() {
		this.stepService.getStep().subscribe((step) => {
			if (step) {
				this.stepId = step.id;
				if (step.stepForm && step.type === 'next') {
					switch (step.id) {
						case 'one': {
							this.step1Control = step.stepForm;
							if (step.data) {
								this.importId = step.data.importId;
								this.srid = step.data.srid;
								this.columns = step.data.columns.map((col) => {
									return {
										id: col,
										selected: false
									};
								});
							}
							this.cd.detectChanges();
							this.stepper.next();
							break;
						}
						case 'two': {
							this.step2Control = step.stepForm;
							this.contentMappingInfo = step.data.content_mapping_info;
							this.table_name = step.data.table_name;
							this.selected_columns = step.data.selected_columns;
							this.added_columns = step.data.added_columns;
							this.cd.detectChanges();
							this.stepper.next();
							break;
						}
						case 'three': {
							this.step3Control = step.stepForm;
							this.cd.detectChanges();
							this.stepper.next();
							break;
						}
					}
				}
				if (step.type === 'previous') {
					this.cd.detectChanges();
					this.stepper.previous();
				}
				if (step.type === 'goTo') {
					this.isLinear = false;
					this.cd.detectChanges();
					this.stepper.selectedIndex = 3;
					this.isLinear = true;
					this.cd.detectChanges();
				}
				if (step.type === 'reset') {
					switch (step.id) {
						case 'one': {
							this.step1Control = this.initForm;
							this.step2Control = this.initForm;
							this.step3Control = this.initForm;
							this.step4Control = this.initForm;
							this.cd.detectChanges();
							break;
						}
						case 'two': {
							this.step2Control = this.initForm;
							this.step3Control = this.initForm;
							this.step4Control = this.initForm;
							this.cd.detectChanges();
							break;
						}
						case 'three': {
							this.step4Control = this.initForm;
							this.cd.detectChanges();
							break;
						}
					}
				}
			}
		});
	}

	cancelImport() {
		this._ds.cancelImport(this.importId).subscribe(
			() => {
				this.importId = null;
				this.srid = null;
				this.table_name = null;
				this.selected_columns = null;
				this.added_columns = null;
				this.stepService.resetStep();
				this._router.navigate([ `${this.IMPORT_CONFIG.MODULE_URL}` ]);
			},
			(error) => {
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					if ((error.status = 400)) {
						this._router.navigate([ `${this.IMPORT_CONFIG.MODULE_URL}` ]);
					}
					// show error message if other server error
					this.toastr.error(error.error);
				}
			}
		);
	}

	onImportList() {
		this._router.navigate([ `${this.IMPORT_CONFIG.MODULE_URL}` ]);
	}


    move(index: number) {
        this.stepper.selectedIndex = index;
      }


}
